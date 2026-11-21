"""PostgreSQL transactions for one isolated repair-time migration rehearsal.

The rehearsal state lives in its own schema together with a control row and
per-batch checkpoints.  A process that stops between batches, or between a
batch's snapshot and its first write, can therefore attach to the same schema
and continue from the last committed checkpoint instead of starting over.

Every attempt carries a token.  Mutating transactions lock the control row and
compare the token before doing anything, so an attempt that has been taken over
cannot advance a batch or publish an acceptance report.  The acceptance report
itself is stored in the control row before it is returned, which makes issuing
it on retry idempotent.
"""

import hashlib
import json
from functools import wraps
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from src.utils.encoding import canonical


class AttemptSuperseded(RuntimeError):
    """The rehearsal has been taken over by a newer attempt."""


# A dropped schema is the strongest form of fencing; the losing attempt must
# stop cleanly instead of surfacing an undefined-table error.
_GONE = {"42P01", "3F000"}


def _fenced(operation):
    @wraps(operation)
    def wrapper(self, *args, **kwargs):
        try:
            return operation(self, *args, **kwargs)
        except ProgrammingError as exc:
            code = getattr(getattr(exc, "orig", None), "sqlstate", None)
            if code in _GONE:
                raise AttemptSuperseded("本次执行已被接管或清理") from None
            raise
    return wrapper


class RepairMigration:
    def __init__(self, engine, schema=None):
        self.engine = engine
        self.schema = schema or f"gb5309_{uuid4().hex}"
        self.orders = f"{self.schema}.repair_orders"
        self.events = f"{self.schema}.change_events"
        self.to_ticks = f"{self.schema}.to_ticks"
        self.control = f"{self.schema}.migration_control"
        self.checkpoints = f"{self.schema}.batch_checkpoints"
        self.race_table = f"{self.schema}.race_evidence"
        self.token = None
        self.run_id = None

    # -- lifecycle ---------------------------------------------------------

    def install(self, orders, events, token=None, fingerprint=""):
        """Create the rehearsal schema and all of its state in one transaction."""
        with self.engine.begin() as conn:
            self._advisory(conn)
            if self._exists(conn):
                raise ValueError("演练 schema 已存在")
            self.token = token
            self._create(conn, orders, events, token, fingerprint)
    def open(self, payload, run_id=None, token=None):
        """Create the rehearsal, or attach to it and take it over.

        Attaching replaces the control row's attempt token.  The previous
        attempt's later transactions no longer match it and are fenced off.
        """
        self.run_id = run_id
        self.token = token or uuid4().hex
        fingerprint = hashlib.sha256(canonical(payload)).hexdigest()
        with self.engine.begin() as conn:
            self._advisory(conn)
            if not self._exists(conn):
                self._check_run_owner(conn, run_id, token)
                self._create(conn, payload["orders"], payload["events"], self.token, fingerprint)
                return
            row = conn.execute(text(f"""
                SELECT attempt_token, payload_fingerprint FROM {self.control} FOR UPDATE
            """)).mappings().first()
            if row is None:
                raise ValueError("演练 schema 缺少控制表，无法继续")
            if row["payload_fingerprint"] and row["payload_fingerprint"] != fingerprint:
                raise ValueError("演练 schema 与本次输入不一致")
            self._check_run_owner(conn, run_id, token)
            conn.execute(text(f"""
                UPDATE {self.control} SET attempt_token = :token WHERE guard
            """), {"token": self.token})

    def close(self):
        with self.engine.begin() as conn:
            self._advisory(conn)
            conn.execute(text(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE"))

    def drop_if_terminal(self):
        """Drop the schema only when its business run has already finished.

        Used on worker restart to clear schemas left behind by a crash between
        publishing the evidence and dropping the rehearsal. A run that is still
        running keeps its schema for the attempt that will resume it.
        """
        if self.run_id is None:
            return False
        with self.engine.begin() as conn:
            self._advisory(conn)
            if not self._exists(conn):
                return False
            if conn.execute(text("SELECT to_regclass('business_runs')")).scalar() is not None:
                status = conn.execute(text("""
                    SELECT status FROM business_runs WHERE id = :id
                """), {"id": self.run_id}).scalar()
                # Only drop when the run is definitely finished. A missing run
                # row means ownership cannot be proven, so keep the schema.
                if status not in ("complete", "failed"):
                    return False
            conn.execute(text(f"DROP SCHEMA {self.schema} CASCADE"))
            return True

    def drop_if_owner(self, token=None):
        """Drop the schema unless another attempt has taken it over."""
        with self.engine.begin() as conn:
            self._advisory(conn)
            if not self._exists(conn):
                return False
            row = conn.execute(text(f"""
                SELECT attempt_token FROM {self.control} FOR UPDATE
            """)).mappings().first()
            if row is None:
                return False
            if token is not None and row["attempt_token"] != token:
                return False
            conn.execute(text(f"DROP SCHEMA {self.schema} CASCADE"))
            return True

    # -- setup helpers -----------------------------------------------------

    def _advisory(self, conn):
        # Serialise schema creation, attachment and removal across processes.
        # A single bigint key lives in a different advisory-lock space from the
        # two-int run locks, so the two can never collide.
        if self.engine.dialect.name == "postgresql":
            conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:name, 5309))"),
                         {"name": self.schema})

    def _exists(self, conn):
        return conn.execute(text("""
            SELECT 1 FROM information_schema.schemata WHERE schema_name = :name
        """), {"name": self.schema}).scalar() is not None

    def _check_run_owner(self, conn, run_id, token, quiet=False):
        if run_id is None or token is None:
            return True
        # The business tables are absent when the service is used directly in
        # a test database; the control-row token still fences attempts there.
        if conn.execute(text("SELECT to_regclass('business_runs')")).scalar() is None:
            return True
        row = conn.execute(text("""
            SELECT status, attempt_token FROM business_runs WHERE id = :id FOR UPDATE
        """), {"id": run_id}).mappings().first()
        if row is None:
            # No authority row to compare against; the control-row token still
            # fences every attempt on this rehearsal.
            return True
        owned = row["status"] == "running" and row["attempt_token"] == token
        if not owned and not quiet:
            raise AttemptSuperseded("本次执行已被接管")
        return owned

    def _create(self, conn, orders, events, token, fingerprint):
        conn.execute(text(f"CREATE SCHEMA {self.schema}"))
        conn.execute(text(f"""
            CREATE TABLE {self.orders} (
                ticket text COLLATE "C" PRIMARY KEY,
                legacy_hours numeric NOT NULL CHECK (legacy_hours BETWEEN 0 AND 168),
                revision integer NOT NULL CHECK (revision > 0)
            )
        """))
        if orders:
            conn.execute(text(f"""
                INSERT INTO {self.orders} (ticket, legacy_hours, revision)
                VALUES (:ticket, CAST(:legacy_hours AS numeric), :revision)
            """), orders)
        conn.execute(text(f"""
            ALTER TABLE {self.orders}
                ADD COLUMN shadow_ticks integer CHECK (shadow_ticks BETWEEN 0 AND 672),
                ADD COLUMN shadow_revision integer CHECK (shadow_revision > 0),
                ADD CHECK ((shadow_ticks IS NULL) = (shadow_revision IS NULL))
        """))
        # An exact quarter-hour conversion. Casting an inexact numeric to integer would round it.
        conn.execute(text(f"""
            CREATE FUNCTION {self.to_ticks}(hours numeric) RETURNS integer
            LANGUAGE sql IMMUTABLE STRICT AS $$
                SELECT CASE WHEN mod(hours * 4, 1) = 0 THEN (hours * 4)::integer END
            $$
        """))
        conn.execute(text(f"""
            CREATE FUNCTION {self.schema}.dual_write() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                NEW.shadow_ticks := {self.to_ticks}(NEW.legacy_hours);
                NEW.shadow_revision := CASE WHEN NEW.shadow_ticks IS NOT NULL THEN NEW.revision END;
                RETURN NEW;
            END;
            $$
        """))
        conn.execute(text(f"""
            CREATE TRIGGER dual_write
            BEFORE INSERT OR UPDATE OF legacy_hours, revision ON {self.orders}
            FOR EACH ROW EXECUTE FUNCTION {self.schema}.dual_write()
        """))
        conn.execute(text(f"""
            CREATE TABLE {self.events} (
                seq integer PRIMARY KEY,
                after_batch integer NOT NULL CHECK (after_batch >= 0),
                ticket text NOT NULL,
                legacy_hours numeric NOT NULL CHECK (legacy_hours BETWEEN 0 AND 168),
                revision integer NOT NULL CHECK (revision > 0),
                status text NOT NULL DEFAULT '待消费',
                observed_revision integer
            )
        """))
        conn.execute(text(f"""
            CREATE INDEX change_events_pending ON {self.events} (after_batch, seq)
            WHERE status = '待消费'
        """))
        if events:
            conn.execute(text(f"""
                INSERT INTO {self.events} (seq, after_batch, ticket, legacy_hours, revision)
                VALUES (:seq, :after_batch, :ticket, CAST(:legacy_hours AS numeric), :revision)
            """), [{**event, "seq": seq} for seq, event in enumerate(events, 1)])
        conn.execute(text(f"""
            CREATE TABLE {self.control} (
                guard boolean PRIMARY KEY DEFAULT true CHECK (guard),
                attempt_token text,
                payload_fingerprint text NOT NULL DEFAULT '',
                cursor text NOT NULL DEFAULT '',
                next_batch integer NOT NULL DEFAULT 1,
                report jsonb
            )
        """))
        conn.execute(text(f"""
            INSERT INTO {self.control} (attempt_token, payload_fingerprint)
            VALUES (:token, :fingerprint)
        """), {"token": token, "fingerprint": fingerprint})
        # The snapshot is committed before the interleaved change is replayed,
        # so a restart repeats the batch from exactly the same source view.
        conn.execute(text(f"""
            CREATE TABLE {self.checkpoints} (
                batch_no integer PRIMARY KEY,
                status text NOT NULL CHECK (status IN ('pending', 'done')),
                snapshot jsonb NOT NULL,
                summary jsonb,
                created_at timestamptz NOT NULL DEFAULT now(),
                finished_at timestamptz
            )
        """))
        # Credentials for snapshots blocked by a concurrent change are part of
        # the batch transaction, so they can never commit separately from it.
        conn.execute(text(f"""
            CREATE TABLE {self.race_table} (
                batch_no integer NOT NULL,
                ordinal integer NOT NULL,
                ticket text NOT NULL,
                payload jsonb NOT NULL,
                PRIMARY KEY (batch_no, ticket)
            )
        """))

    # -- fencing -----------------------------------------------------------

    def _lock_and_fence(self, conn):
        row = conn.execute(text(f"""
            SELECT attempt_token, cursor, next_batch, report
            FROM {self.control} WHERE guard FOR UPDATE
        """)).mappings().first()
        if row is None:
            raise AttemptSuperseded("本次执行已被接管或清理")
        if self.token is not None and row["attempt_token"] != self.token:
            raise AttemptSuperseded("本次执行已被接管")
        if self.run_id is not None:
            self._check_run_owner(conn, self.run_id, self.token)
        return row

    # -- resume state ------------------------------------------------------

    @_fenced
    def progress(self):
        with self.engine.connect() as conn:
            row = conn.execute(text(f"""
                SELECT cursor, next_batch FROM {self.control} WHERE guard
            """)).mappings().first()
        if row is None:
            raise AttemptSuperseded("本次执行已被清理")
        return {"cursor": row["cursor"], "next_batch": row["next_batch"]}

    @_fenced
    def stored_report(self):
        with self.engine.connect() as conn:
            return conn.execute(text(f"""
                SELECT report FROM {self.control} WHERE guard
            """)).scalar()

    # -- replay and backfill ----------------------------------------------

    @_fenced
    def replay(self, batch_no):
        # The legacy update, the trigger's shadow write, and event acknowledgement commit together.
        with self.engine.begin() as conn:
            self._lock_and_fence(conn)
            events = conn.execute(text(f"""
                SELECT seq, ticket, legacy_hours, revision
                FROM {self.events}
                WHERE after_batch = :batch AND status = '待消费'
                ORDER BY seq FOR UPDATE
            """), {"batch": batch_no}).mappings().all()
            for event in events:
                revision = conn.execute(text(f"""
                    INSERT INTO {self.orders} AS target (ticket, legacy_hours, revision)
                    VALUES (:ticket, :legacy_hours, :revision)
                    ON CONFLICT (ticket) DO UPDATE
                    SET legacy_hours = EXCLUDED.legacy_hours, revision = EXCLUDED.revision
                    WHERE target.revision < EXCLUDED.revision
                    RETURNING revision
                """), event).scalar_one_or_none()
                status = "双写已消费"
                if revision is None:
                    revision, same_hours = conn.execute(text(f"""
                        SELECT revision, legacy_hours = :legacy_hours FROM {self.orders}
                        WHERE ticket = :ticket
                    """), event).one()
                    if revision > event["revision"]:
                        status = "过期已消费"
                    else:
                        status = "重复已消费" if same_hours else "同版冲突"
                conn.execute(text(f"""
                    UPDATE {self.events} SET status = :status, observed_revision = :revision
                    WHERE seq = :seq
                """), {"status": status, "revision": revision, "seq": event["seq"]})

    @_fenced
    def backfill(self, cursor, batch_size, batch_no, replay_due):
        """Run one batch, resuming the committed snapshot of an interrupted one."""
        checkpoint = self._prepare(cursor, batch_size, batch_no)
        if checkpoint is None:
            return None
        if checkpoint["status"] == "done":
            return checkpoint["summary"], self.race_rows(batch_no)
        # No row locks are held by the snapshot. Replay commits on a separate connection.
        if replay_due:
            self.replay(batch_no)
        return self._commit(batch_no)

    def _prepare(self, cursor, batch_size, batch_no):
        with self.engine.begin() as conn:
            control = self._lock_and_fence(conn)
            existing = conn.execute(text(f"""
                SELECT status, snapshot, summary FROM {self.checkpoints}
                WHERE batch_no = :batch FOR UPDATE
            """), {"batch": batch_no}).mappings().first()
            if existing is not None:
                return dict(existing)
            if batch_no != control["next_batch"]:
                raise ValueError(f"批次 {batch_no} 不是当前待执行批次")
            snapshot = conn.execute(text(f"""
                SELECT ticket, revision, {self.to_ticks}(legacy_hours) AS ticks,
                       shadow_revision IS NULL OR shadow_revision < revision AS needs_backfill
                FROM {self.orders}
                WHERE ticket > :cursor ORDER BY ticket LIMIT :batch_size
            """), {"cursor": cursor, "batch_size": batch_size}).mappings().all()
            if not snapshot:
                return None
            rows = [dict(row) for row in snapshot]
            conn.execute(text(f"""
                INSERT INTO {self.checkpoints} (batch_no, status, snapshot)
                VALUES (:batch, 'pending', CAST(:snapshot AS jsonb))
            """), {"batch": batch_no, "snapshot": json.dumps(rows, ensure_ascii=False)})
            return {"status": "pending", "snapshot": rows, "summary": None}

    def _commit(self, batch_no):
        with self.engine.begin() as conn:
            self._lock_and_fence(conn)
            checkpoint = conn.execute(text(f"""
                SELECT status, snapshot, summary FROM {self.checkpoints}
                WHERE batch_no = :batch FOR UPDATE
            """), {"batch": batch_no}).mappings().first()
            if checkpoint is None:
                raise ValueError(f"批次 {batch_no} 缺少检查点")
            if checkpoint["status"] == "done":
                return checkpoint["summary"], self._races(conn, batch_no)
            snapshot = checkpoint["snapshot"]
            candidates = [row for row in snapshot if row["ticks"] is not None and row["needs_backfill"]]
            written = self.write_snapshot(conn, candidates)
            races = self.race_evidence(conn, batch_no, [row for row in candidates if row["ticket"] not in written])
            for ordinal, race in enumerate(races, 1):
                conn.execute(text(f"""
                    INSERT INTO {self.race_table} (batch_no, ordinal, ticket, payload)
                    VALUES (:batch, :ordinal, :ticket, CAST(:payload AS jsonb))
                """), {"batch": batch_no, "ordinal": ordinal, "ticket": race["工单号"],
                       "payload": json.dumps(race, ensure_ascii=False)})
            summary = {
                "批次": batch_no,
                "起始工单": snapshot[0]["ticket"],
                "最后工单": snapshot[-1]["ticket"],
                "扫描工单": len(snapshot),
                "回填写入": len(written),
                "竞态拦截": len(races),
                "快照不可转换": sum(row["ticks"] is None for row in snapshot),
                "快照已同步": sum(row["ticks"] is not None and not row["needs_backfill"] for row in snapshot),
            }
            # The shadow values, blocked-snapshot credentials, batch summary and
            # cursor commit as one unit, so a restart can never half-apply a batch.
            conn.execute(text(f"""
                UPDATE {self.checkpoints} SET status = 'done', summary = CAST(:summary AS jsonb),
                    finished_at = now()
                WHERE batch_no = :batch
            """), {"batch": batch_no, "summary": json.dumps(summary, ensure_ascii=False)})
            conn.execute(text(f"""
                UPDATE {self.control} SET cursor = :cursor, next_batch = :next_batch WHERE guard
            """), {"cursor": snapshot[-1]["ticket"], "next_batch": batch_no + 1})
            return summary, races

    @_fenced
    def race_rows(self, batch_no):
        with self.engine.connect() as conn:
            return self._races(conn, batch_no)

    def _races(self, conn, batch_no):
        return [row["payload"] for row in conn.execute(text(f"""
            SELECT payload FROM {self.race_table}
            WHERE batch_no = :batch ORDER BY ordinal
        """), {"batch": batch_no}).mappings()]

    def write_snapshot(self, conn, candidates):
        if not candidates:
            return set()
        return set(conn.execute(text(f"""
            UPDATE {self.orders} AS target
            SET shadow_ticks = snapshot.ticks, shadow_revision = snapshot.revision
            FROM jsonb_to_recordset(CAST(:snapshot AS jsonb)) AS snapshot(ticket text, revision integer, ticks integer)
            WHERE target.ticket = snapshot.ticket AND target.revision = snapshot.revision
              AND (target.shadow_revision IS NULL OR target.shadow_revision < snapshot.revision)
            RETURNING target.ticket
        """), {"snapshot": json.dumps(candidates, ensure_ascii=False)}).scalars())

    def race_evidence(self, conn, batch_no, skipped):
        if not skipped:
            return []
        current = conn.execute(text(f"""
            SELECT ticket, revision, shadow_ticks, shadow_revision FROM {self.orders}
            WHERE ticket = ANY(CAST(:tickets AS text[]))
        """), {"tickets": [row["ticket"] for row in skipped]}).mappings()
        by_ticket = {row["ticket"]: row for row in current}
        return [{
            "批次": batch_no,
            "工单号": row["ticket"],
            "快照版次": row["revision"],
            "快照刻度": row["ticks"],
            "更新行数": 0,
            "当前版次": by_ticket[row["ticket"]]["revision"],
            "影子版次": by_ticket[row["ticket"]]["shadow_revision"],
            "保留刻度": by_ticket[row["ticket"]]["shadow_ticks"],
        } for row in skipped]

    # -- acceptance --------------------------------------------------------

    @_fenced
    def finalize(self, build):
        """Build the acceptance report once, store it, then return it.

        Every writer takes the control row lock before touching orders or
        events, so holding it here makes the gate reads stable without a
        repeatable-read snapshot (which could raise a serialisation error on
        the lock instead). The report is written in the same transaction: a
        retry after a crash before publication returns the already computed
        report unchanged.
        """
        with self.engine.begin() as conn:
            control = self._lock_and_fence(conn)
            if control["report"] is not None:
                return control["report"]
            orders, events = self._read_gate(conn)
            batches = [row["summary"] for row in conn.execute(text(f"""
                SELECT summary FROM {self.checkpoints} WHERE status = 'done' ORDER BY batch_no
            """)).mappings()]
            races = [row["payload"] for row in conn.execute(text(f"""
                SELECT payload FROM {self.race_table} ORDER BY batch_no, ordinal
            """)).mappings()]
            report = build(orders, events, batches, races)
            conn.execute(text(f"""
                UPDATE {self.control} SET report = CAST(:report AS jsonb) WHERE guard
            """), {"report": json.dumps(report, ensure_ascii=False)})
            return report

    @_fenced
    def read_gate(self):
        # Check the source, shadow and event queue against a single committed snapshot.
        with self.engine.connect().execution_options(isolation_level="REPEATABLE READ") as conn, conn.begin():
            return self._read_gate(conn)

    def _read_gate(self, conn):
        orders = conn.execute(text(f"""
            SELECT ticket, legacy_hours::text AS legacy_hours, revision, shadow_ticks, shadow_revision,
                CASE
                    WHEN mod(legacy_hours, 0.25) <> 0 THEN '不可转换'
                    WHEN shadow_ticks IS NULL THEN '待回填'
                    WHEN shadow_revision IS DISTINCT FROM revision THEN '版次不一致'
                    WHEN shadow_ticks * 0.25 <> legacy_hours THEN '刻度不一致'
                    ELSE '一致'
                END AS status
            FROM {self.orders} ORDER BY ticket
        """)).mappings().all()
        events = conn.execute(text(f"""
            SELECT seq, after_batch, ticket, legacy_hours::text AS legacy_hours,
                   revision, status, observed_revision
            FROM {self.events} ORDER BY seq
        """)).mappings().all()
        return orders, events
