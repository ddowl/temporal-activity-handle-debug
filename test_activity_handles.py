import asyncio
from datetime import timedelta

import pytest
from temporalio import activity, workflow
from temporalio.client import Client as TemporalClient

from conftest import temporal_worker


@activity.defn()
def dummy_handle_activity() -> None:
    pass


@workflow.defn(sandboxed=False)
class TestActivityHandlesWf:
    @workflow.run
    async def run(self) -> str:
        activity_handle = await workflow.start_activity(dummy_handle_activity, schedule_to_close_timeout=timedelta(seconds=10))
        print(activity_handle)
        assert activity_handle is not None
        asyncio.gather(activity_handle)
        return "finished running activities!"


@pytest.mark.asyncio
async def test_activity_handles(temporal_client: TemporalClient):
    async with temporal_worker(temporal_client, workflows=[TestActivityHandlesWf], activities=[dummy_handle_activity]) as tq:
        res = await temporal_client.execute_workflow(
            TestActivityHandlesWf.run, id="test-activity-handles", task_queue=tq, run_timeout=timedelta(seconds=10), rpc_timeout=timedelta(seconds=15)
        )
        assert res == "finished running activities!"
