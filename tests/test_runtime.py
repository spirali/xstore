import pytest

from orco import LocalExecutor


def test_runtime_stop(env):
    with env.test_runtime() as runtime:
        executor = LocalExecutor(heartbeat_interval=1)
        runtime.register_executor(executor)
        assert not runtime.stopped

    runtime = env.test_runtime()
    with runtime:
        executor = LocalExecutor(heartbeat_interval=1)
        runtime.register_executor(executor)
        assert not runtime.stopped

    with pytest.raises(Exception):
        with runtime:
            pass
