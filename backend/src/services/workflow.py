import hashlib
import threading
import uuid

from fastapi import HTTPException
from sqlalchemy import and_, or_, select, text

from src.config.settings import cache, engine
from src.constants.workflow import Status
from src.models.tables import audit, cases, evidence, runs
from src.services import domain
from src.utils.encoding import canonical
from src.utils.time import utc_time

# A running attempt holds a PostgreSQL session advisory lock for the whole run.
# A restarted worker can take the run over as soon as the dead session is gone,
# while a live worker keeps its own run out of every other worker's hands.
ADVISORY_NAMESPACE = 5309
# Fallback lease for engines without advisory locks (for example MySQL).
STALE_RUN_SECONDS = 600
_locks = {}
_locks_guard = threading.Lock()


def log(conn, actor, action, reference):
    conn.execute(audit.insert().values(actor=actor, action=action, reference=str(reference), created_at=utc_time()))


def recent(table):
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(select(table).order_by(table.c.id.desc()).limit(100)).mappings()]


def found(conn, table, identity):
    row = conn.execute(select(table).where(table.c.id == identity)).mappings().first()
    if row is None:
        raise HTTPException(404, '找不到该记录')
    return dict(row)


def make_case(title, payload, actor):
    title = title.strip()
    if not title:
        raise HTTPException(422, '档案名称不能为空')
    try:
        if len(canonical(payload)) > 512 * 1024:
            raise ValueError('业务输入超过 512 KiB')
        if callable(getattr(domain, 'validate', None)):
            domain.validate(payload)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc)) from None
    with engine.begin() as conn:
        case_id = conn.execute(cases.insert().values(slug=uuid.uuid4().hex, title=title, payload=payload,
                               author=actor, created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'create_case', case_id)
        return found(conn, cases, case_id)


def enqueue(case_id, actor):
    with engine.begin() as conn:
        found(conn, cases, case_id)
        run_id = conn.execute(runs.insert().values(case_id=case_id, status=Status.QUEUED,
                              owner=actor, created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'enqueue', run_id)
        result = found(conn, runs, run_id)
    try:
        cache.lpush('worker:wake', 'queued')
        cache.ltrim('worker:wake', 0, 99)
    except Exception:
        # The durable database queue is polled even if Redis is temporarily unavailable.
        pass
    return result


def _hold(conn, run_id):
    """Try to hold the run's session advisory lock on a dedicated connection."""
    try:
        got = conn.execute(text('SELECT pg_try_advisory_lock(:ns, :id)'),
                           {'ns': ADVISORY_NAMESPACE, 'id': run_id}).scalar()
    except Exception:
        return False
    if got:
        with _locks_guard:
            _locks[run_id] = conn
    return bool(got)


def _unlock(conn, run_id):
    conn.execute(text('SELECT pg_advisory_unlock(:ns, :id)'), {'ns': ADVISORY_NAMESPACE, 'id': run_id})


def _release(run_id):
    with _locks_guard:
        conn = _locks.pop(run_id, None)
    if conn is not None:
        try:
            _unlock(conn, run_id)
        finally:
            conn.close()


def claim():
    """Claim the next run, recovering an abandoned one when its worker is gone.

    PostgreSQL: a live attempt holds a session advisory lock for the whole run.
    A queued run is claimed directly; a running run is taken over as soon as the
    lock is free, which happens the moment the previous worker's database
    session ends. Taking over installs a fresh attempt token, fencing the old
    worker off every later batch write and acceptance publication.
    """
    if engine.dialect.name != 'postgresql':
        return _claim_lease()
    guard = engine.connect().execution_options(isolation_level='AUTOCOMMIT')
    kept = False
    try:
        candidates = guard.execute(text("""
            SELECT id FROM business_runs WHERE status IN ('queued', 'running')
            ORDER BY (status = 'running'), id LIMIT 50
        """)).scalars().all()
        for run_id in candidates:
            if not _hold(guard, run_id):
                continue
            token = str(uuid.uuid4())
            row = guard.execute(text("""
                UPDATE business_runs
                SET status = 'running', started_at = :now, attempt_token = :token
                WHERE id = :id AND status IN ('queued', 'running')
                RETURNING case_id
            """), {'now': utc_time(), 'token': token, 'id': run_id}).mappings().first()
            if row is None:
                _unlock(guard, run_id)
                _locks.pop(run_id, None)
                continue
            kept = True
            payload = found(guard, cases, row['case_id'])['payload']
            return run_id, token, payload
        return None
    except Exception:
        # Never leave a claimed lock behind when the payload read fails.
        with _locks_guard:
            held = list(_locks)
        for run_id in held:
            _release(run_id)
        raise
    finally:
        if not kept:
            guard.close()


def _claim_lease():
    """Lease-only claim for engines without advisory locks."""
    ready = or_(runs.c.status == Status.QUEUED,
                and_(runs.c.status == Status.RUNNING, runs.c.started_at < utc_time(-STALE_RUN_SECONDS)))
    with engine.begin() as conn:
        run = conn.execute(select(runs.c.id, runs.c.case_id).where(ready).order_by(runs.c.id)
                           .limit(1).with_for_update(skip_locked=True)).first()
        if not run:
            return None
        token = str(uuid.uuid4())
        conn.execute(runs.update().where(runs.c.id == run.id).values(
            started_at=utc_time(), status=Status.RUNNING, attempt_token=token))
        return run.id, token, found(conn, cases, run.case_id)['payload']


def release(run_id):
    _release(run_id)


def finish(run_id, token, result=None, error=None):
    digest = hashlib.sha256(canonical(result)).hexdigest() if result is not None else None
    status = Status.FAILED if error else Status.COMPLETE
    with engine.begin() as conn:
        # Only the attempt that still owns the run may publish its outcome.
        count = conn.execute(runs.update().where(runs.c.id == run_id,
                        runs.c.attempt_token == token, runs.c.status == Status.RUNNING)
                        .values(status=status, finished_at=utc_time(), error=error)).rowcount
        if not count:
            return False
        if result is not None:
            # The run row is unique per evidence. If an earlier attempt computed
            # the acceptance order but stopped before inserting it, the retry
            # publishes exactly the same report once.
            conn.execute(evidence.insert().values(run_id=run_id, report=result, digest=digest))
        log(conn, 'worker', status.value, run_id)
        return True


def review(identity, note, actor):
    note = note.strip()
    if not note:
        raise HTTPException(422, '复核意见不能为空')
    with engine.begin() as conn:
        found(conn, evidence, identity)
        count = conn.execute(evidence.update().where(evidence.c.id == identity, evidence.c.reviewed_at.is_(None))
                            .values(review_note=note, reviewer=actor, reviewed_at=utc_time())).rowcount
        if count != 1:
            raise HTTPException(409, '该证据已经复核')
        log(conn, actor, 'review', identity)
        return found(conn, evidence, identity)
