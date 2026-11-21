import logging
from pathlib import Path
from threading import Event, Thread
from time import sleep

from sqlalchemy import text

from src.config.settings import ARTIFACTS, cache, engine
from src.services.domain import cleanup, solve, sweep
from src.services.repair_migration import AttemptSuperseded
from src.services.workflow import claim, finish, release

HEARTBEAT = Path(ARTIFACTS) / 'worker-heartbeat'


def heartbeat(stop):
    while not stop.is_set():
        HEARTBEAT.touch()
        stop.wait(2)


def recover():
    """Clear rehearsal schemas whose run already finished before a crash."""
    try:
        with engine.connect() as conn:
            orphaned = conn.execute(text("""
                SELECT schema_name FROM information_schema.schemata
                WHERE schema_name LIKE 'gb5309\\_run\\_%'
            """)).scalars().all()
        run_ids = [int(name.rsplit('_', 1)[1]) for name in orphaned if name.rsplit('_', 1)[1].isdigit()]
        if run_ids:
            sweep(engine, run_ids)
    except Exception:
        logging.exception('Startup recovery failed')


def work():
    HEARTBEAT.unlink(missing_ok=True)
    recover()
    while True:
        try:
            job = claim()
            HEARTBEAT.touch()
            if not job:
                cache.brpop('worker:wake', timeout=1)
                continue
            run_id, token, payload = job
            stop = Event()
            thread = Thread(target=heartbeat, args=(stop,), daemon=True)
            thread.start()
            try:
                result = solve(payload, engine, run_id, token)
                # The report is already durable in the rehearsal schema. Publishing
                # it and, only then, dropping the schema keeps a crash in between a
                # retry of the same, unchanged acceptance order.
                if finish(run_id, token, result=result):
                    cleanup(run_id, engine, token)
            except AttemptSuperseded:
                # A newer attempt owns the run now; leave its state untouched.
                pass
            except (ValueError, KeyError, TypeError) as exc:
                if finish(run_id, token, error=str(exc)[:500]):
                    cleanup(run_id, engine, token)
            except Exception:
                logging.exception('Domain run failed: %s', run_id)
                if finish(run_id, token, error='后台执行失败，请检查输入或查看 worker 日志。'):
                    cleanup(run_id, engine, token)
            finally:
                stop.set()
                thread.join()
                release(run_id)
        except Exception:
            logging.exception('Worker loop failure')
            sleep(2)


if __name__ == '__main__':
    work()
