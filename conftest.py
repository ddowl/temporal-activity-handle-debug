# pylint: disable=redefined-outer-name,missing-docstring,dangerous-default-value
import asyncio
import contextlib
import logging
import os
import typing
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import AsyncGenerator, Callable, Optional, Sequence, Type
from unittest import mock

import pytest
import pytest_asyncio
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

logger = logging.getLogger()

DEV_SERVER_INIT_RETRIES = 10


@pytest.fixture(scope="session")
def event_loop():
    """ Create an event loop for use during testing """
    logger.debug("Creating test event loop...")
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.set_debug(True)
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def temporal_env() -> AsyncGenerator[WorkflowEnvironment, None]:
    """ Fetches and starts Temporal dev server binary process in the background. """

    logger.debug("Fetching and starting Temporal dev server...")
    env = await download_and_init_temporal_env()
    logger.debug("Temporal dev server initialized.")
    yield env
    logger.debug("shutting down Temporal dev server...")
    await env.shutdown()
    logger.debug("Temporal dev server shut down.")


async def download_and_init_temporal_env() -> WorkflowEnvironment:
    err = None
    for _ in range(DEV_SERVER_INIT_RETRIES):
        try:
            return await WorkflowEnvironment.start_local(dev_server_log_level="fatal")
        except RuntimeError as e:
            err = e
            if "Failed connecting to test server" not in str(e):
                raise e
    raise typing.cast(RuntimeError, err)  # err is guaranteed to be set by this point


@pytest_asyncio.fixture()
async def temporal_client(temporal_env: WorkflowEnvironment) -> Client:
    """ Get client for temporal test server. """
    return temporal_env.client



@contextlib.asynccontextmanager
async def temporal_worker(
    client: Client,
    *,
    workflows: Sequence[Type],
    activities: Sequence[Callable],
    task_queue: Optional[str] = None,
):
    if not task_queue:
        task_queue = f"task_queue-{str(uuid.uuid4())}"

    num_cpus = cpus if (cpus := os.cpu_count()) else 0
    max_activity_threads = min(32, num_cpus + 4)
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        activities=activities,
        activity_executor=ThreadPoolExecutor(max_workers=max_activity_threads),
        max_concurrent_activities=max_activity_threads,
    ):
        yield task_queue

    # Terminate running workflows on this task queue at the end of each test
    async for workflow_execution in client.list_workflows():
        if (
            workflow_execution.task_queue == task_queue
            and workflow_execution.status == WorkflowExecutionStatus.RUNNING
        ):
            await client.get_workflow_handle(workflow_execution.id).terminate()