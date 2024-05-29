from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest

from dbt.flags import get_flags
from dbt.task.run import RunTask
from dbt.tests.util import safe_set_invocation_context


@pytest.mark.parametrize(
    "exception_to_raise, expected_cancel_connections",
    [
        (SystemExit, True),
        (KeyboardInterrupt, True),
        (Exception, False),
    ],
)
def test_run_task_cancel_connections(exception_to_raise, expected_cancel_connections):
    safe_set_invocation_context()

    def mock_run_queue(*args, **kwargs):
        raise exception_to_raise("Test exception")

    with patch.object(RunTask, "run_queue", mock_run_queue), patch.object(
        RunTask, "_cancel_connections"
    ) as mock_cancel_connections:

        # TODO clean up this after we have a proper runtime config fixture
        # https://github.com/dbt-labs/dbt-core/pull/10242
        flags = get_flags()
        object.__setattr__(flags, "write_json", False)
        task = RunTask(
            flags,
            Namespace(
                threads=1,
                target_name="test",
            ),
            None,
        )
        with pytest.raises(exception_to_raise):
            task.execute_nodes()
        assert mock_cancel_connections.called == expected_cancel_connections


def test_run_task_preserve_edges():
    mock_node_selector = MagicMock()
    mock_spec = MagicMock()
    with patch.object(RunTask, "get_node_selector", return_value=mock_node_selector), patch.object(
        RunTask, "get_selection_spec", return_value=mock_spec
    ):
        task = RunTask(get_flags(), None, None)
        task.get_graph_queue()
        # when we get the graph queue, preserve_edges is True
        mock_node_selector.get_graph_queue.assert_called_with(mock_spec, True)
