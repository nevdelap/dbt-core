from unittest.mock import MagicMock

import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.artifacts.schemas.run import RunResult
from dbt.contracts.graph.nodes import ModelNode
from dbt.events.types import LogModelResult
from dbt.task.run import ModelRunner
from dbt_common.events.base_types import EventLevel
from dbt_common.events.event_manager_client import get_event_manager
from tests.utils import EventCatcher


class TestModelRunner:
    @pytest.fixture
    def log_model_result_catcher(self) -> EventCatcher:
        return EventCatcher(event_to_catch=LogModelResult)

    @pytest.fixture
    def model_runner(
        self,
        mock_adapter: MagicMock,
        table_model: ModelNode,
        mock_project: MagicMock,
    ) -> ModelRunner:
        return ModelRunner(
            config=mock_project, adapter=mock_adapter, node=table_model, node_index=1, num_nodes=1
        )

    @pytest.fixture
    def run_result(self, table_model: ModelNode) -> RunResult:
        return RunResult(
            status=RunStatus.Success,
            timing=[],
            thread_id="an_id",
            execution_time=0,
            adapter_response={},
            message="It did it",
            failures=None,
            node=table_model,
        )

    def test_print_result_line(
        self,
        log_model_result_catcher: EventCatcher,
        model_runner: ModelRunner,
        run_result: RunResult,
    ) -> None:
        # Setup way to catch events
        event_manager = get_event_manager()
        event_manager.callbacks.append(log_model_result_catcher.catch)

        # Check `print_result_line` with "successful" RunResult
        model_runner.print_result_line(run_result)
        assert len(log_model_result_catcher.caught_events) == 1
        assert log_model_result_catcher.caught_events[0].info.level == EventLevel.INFO
        assert log_model_result_catcher.caught_events[0].data.status == run_result.message

        # reset event catcher
        log_model_result_catcher.flush()

        # Check `print_result_line` with "error" RunResult
        run_result.status = RunStatus.Error
        model_runner.print_result_line(run_result)
        assert len(log_model_result_catcher.caught_events) == 1
        assert log_model_result_catcher.caught_events[0].info.level == EventLevel.ERROR
        assert log_model_result_catcher.caught_events[0].data.status == EventLevel.ERROR
