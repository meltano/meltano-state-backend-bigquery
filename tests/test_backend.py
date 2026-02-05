from __future__ import annotations

import shutil
from decimal import Decimal
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from meltano.core.project import Project
from meltano.core.state_store import MeltanoState, state_store_manager_from_project_settings
from meltano.core.state_store.base import (
    MissingStateBackendSettingsError,
    StateIDLockedError,
)

from meltano_state_backend_bigquery.backend import BigQueryStateStoreManager

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from google.cloud import bigquery


@pytest.fixture
def project(tmp_path: Path) -> Project:
    path = tmp_path / "project"
    shutil.copytree(
        "fixtures/project",
        path,
        ignore=shutil.ignore_patterns(".meltano/**"),
    )
    return Project.find(path.resolve())  # type: ignore[no-any-return]


def test_get_manager(project: Project) -> None:
    with (
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_dataset",
        ) as mock_ensure_dataset,
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_tables",
        ) as mock_ensure_tables,
    ):
        manager = state_store_manager_from_project_settings(project.settings)

    mock_ensure_dataset.assert_called_once()
    mock_ensure_tables.assert_called_once()
    assert isinstance(manager, BigQueryStateStoreManager)
    assert manager.uri == "bigquery://my-project"
    assert manager.project == "my-project"
    assert manager.dataset == "test_dataset"
    assert manager.location == "US"


@pytest.mark.parametrize(
    ("setting_name", "env_var_name"),
    (
        pytest.param(
            "state_backend.bigquery.project",
            "MELTANO_STATE_BACKEND_BIGQUERY_PROJECT",
            id="project",
        ),
        pytest.param(
            "state_backend.bigquery.dataset",
            "MELTANO_STATE_BACKEND_BIGQUERY_DATASET",
            id="dataset",
        ),
        pytest.param(
            "state_backend.bigquery.location",
            "MELTANO_STATE_BACKEND_BIGQUERY_LOCATION",
            id="location",
        ),
        pytest.param(
            "state_backend.bigquery.credentials_path",
            "MELTANO_STATE_BACKEND_BIGQUERY_CREDENTIALS_PATH",
            id="credentials_path",
        ),
    ),
)
def test_settings(project: Project, setting_name: str, env_var_name: str) -> None:
    setting = project.settings.find_setting(setting_name)
    assert setting is not None

    env_vars = setting.env_vars(prefixes=["meltano"])
    assert env_vars[0].key == env_var_name


@pytest.fixture
def mock_client() -> Generator[mock.Mock, None, None]:
    """Mock BigQuery client."""
    with mock.patch("google.cloud.bigquery.Client") as mock_client_class:
        mock_client_instance = mock.Mock()
        mock_client_class.return_value = mock_client_instance
        yield mock_client_instance


@pytest.fixture
def subject(
    mock_client: mock.Mock,
) -> tuple[BigQueryStateStoreManager, mock.Mock]:
    """Create BigQueryStateStoreManager instance with mocked client."""
    with (
        mock.patch("google.cloud.bigquery.Client") as mock_client_class,
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_dataset",
        ),
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_tables",
        ),
    ):
        mock_client_class.return_value = mock_client
        manager = BigQueryStateStoreManager(
            uri="bigquery://testproject/testdataset",
            project="testproject",
            dataset="testdataset",
        )
        # Replace the cached client with our mock
        manager.__dict__["client"] = mock_client
        return manager, mock_client


def test_set_state(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test setting state."""
    manager, mock_client = subject

    # Mock query result
    mock_query_job = mock.Mock()
    mock_query_job.result.return_value = []
    mock_client.query.return_value = mock_query_job

    # Test setting new state
    state = MeltanoState(
        state_id="test_job",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    manager.set(state)

    # Verify MERGE query was executed
    mock_client.query.assert_called_once()
    call_args = mock_client.query.call_args
    assert "MERGE `testproject.testdataset.meltano_state`" in call_args[0][0]


def test_get_state(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test getting state."""
    manager, mock_client = subject

    # Mock query result - value column contains both partial and completed
    mock_row = mock.Mock()
    mock_row.value = {
        "partial": {"singer_state": {"partial": 1}},
        "completed": {"singer_state": {"complete": 1}},
    }

    mock_query_job = mock.Mock()
    mock_query_job.result.return_value = [mock_row]
    mock_client.query.return_value = mock_query_job

    # Get state
    state = manager.get("test_job")
    assert state is not None

    # Verify query
    mock_client.query.assert_called_once()
    call_args = mock_client.query.call_args
    assert "FROM `testproject.testdataset.meltano_state`" in call_args[0][0]

    # Verify returned state
    assert state.state_id == "test_job"
    assert state.partial_state == {"singer_state": {"partial": 1}}
    assert state.completed_state == {"singer_state": {"complete": 1}}


def test_get_state_with_null_values(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test getting state with NULL partial_state in value."""
    manager, mock_client = subject

    # Mock query result with None for partial inside value
    mock_row = mock.Mock()
    mock_row.value = {
        "partial": None,
        "completed": {"singer_state": {"complete": 1}},
    }

    mock_query_job = mock.Mock()
    mock_query_job.result.return_value = [mock_row]
    mock_client.query.return_value = mock_query_job

    # Get state
    state = manager.get("test_job")
    assert state is not None

    # Verify returned state handles None correctly
    assert state.state_id == "test_job"
    assert state.partial_state == {}
    assert state.completed_state == {"singer_state": {"complete": 1}}


def test_get_state_not_found(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test getting state that doesn't exist."""
    manager, mock_client = subject

    # Mock query result with no rows
    mock_query_job = mock.Mock()
    mock_query_job.result.return_value = []
    mock_client.query.return_value = mock_query_job

    # Get state
    state = manager.get("nonexistent")

    # Verify it returns None
    assert state is None


def test_get_state_with_none_values(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test getting state with NULL value column in BigQuery."""
    manager, mock_client = subject

    # Mock query result with None value (entire JSON column is NULL)
    mock_row = mock.Mock()
    mock_row.value = None

    mock_query_job = mock.Mock()
    mock_query_job.result.return_value = [mock_row]
    mock_client.query.return_value = mock_query_job

    # Get state
    state = manager.get("test_job")
    assert state is not None

    # Verify returned state has empty dicts for None values
    assert state.state_id == "test_job"
    assert state.partial_state == {}
    assert state.completed_state == {}


def test_delete_state(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test deleting state."""
    manager, mock_client = subject

    # Mock query result
    mock_query_job = mock.Mock()
    mock_query_job.result.return_value = []
    mock_client.query.return_value = mock_query_job

    # Delete state
    manager.delete("test_job")

    # Verify DELETE query
    mock_client.query.assert_called_once()
    call_args = mock_client.query.call_args
    assert "DELETE FROM `testproject.testdataset.meltano_state`" in call_args[0][0]


def test_get_state_ids(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test getting all state IDs."""
    manager, mock_client = subject

    # Mock query result
    mock_rows = [
        mock.Mock(state_id="job1"),
        mock.Mock(state_id="job2"),
        mock.Mock(state_id="job3"),
    ]

    mock_query_job = mock.Mock()
    mock_query_job.result.return_value = mock_rows
    mock_client.query.return_value = mock_query_job

    # Get state IDs
    state_ids = list(manager.get_state_ids())

    # Verify query
    mock_client.query.assert_called_once()
    call_args = mock_client.query.call_args
    assert "SELECT state_id" in call_args[0][0]
    assert "FROM `testproject.testdataset.meltano_state`" in call_args[0][0]

    # Verify returned IDs
    assert state_ids == ["job1", "job2", "job3"]


def test_get_state_ids_with_pattern(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test getting state IDs with pattern."""
    manager, mock_client = subject

    # Mock query result
    mock_rows = [
        mock.Mock(state_id="test_job_1"),
        mock.Mock(state_id="test_job_2"),
    ]

    mock_query_job = mock.Mock()
    mock_query_job.result.return_value = mock_rows
    mock_client.query.return_value = mock_query_job

    # Get state IDs with pattern
    state_ids = list(manager.get_state_ids("test_*"))

    # Verify query with LIKE
    mock_client.query.assert_called_once()
    call_args = mock_client.query.call_args
    assert "SELECT state_id" in call_args[0][0]
    assert "WHERE state_id LIKE" in call_args[0][0]

    # Verify returned IDs
    assert state_ids == ["test_job_1", "test_job_2"]


def test_clear_all(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test clearing all states."""
    manager, mock_client = subject

    # Mock query results
    mock_count_row = mock.Mock(count=5)
    mock_count_job = mock.Mock()
    mock_count_job.result.return_value = [mock_count_row]

    mock_delete_job = mock.Mock()
    mock_delete_job.result.return_value = []

    mock_client.query.side_effect = [mock_count_job, mock_delete_job]

    # Clear all
    count = manager.clear_all()

    # Verify queries
    assert mock_client.query.call_count == 2
    count_call = mock_client.query.call_args_list[0]
    delete_call = mock_client.query.call_args_list[1]

    assert "SELECT COUNT(*)" in count_call[0][0]
    assert "DELETE FROM `testproject.testdataset.meltano_state`" in delete_call[0][0]

    # Verify returned count
    assert count == 5


def test_acquire_lock(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test acquiring and releasing lock."""
    manager, mock_client = subject

    # Mock query results for new MERGE approach
    mock_merge_job = mock.Mock()
    mock_merge_job.result.return_value = []

    # Mock the ownership check - returns our lock_id
    mock_check_row = mock.Mock()
    mock_check_row.lock_id = mock.ANY  # Will match the generated UUID
    mock_check_job = mock.Mock()
    mock_check_job.result.return_value = [mock_check_row]

    mock_delete_job = mock.Mock()
    mock_delete_job.result.return_value = []

    mock_cleanup_job = mock.Mock()
    mock_cleanup_job.result.return_value = []

    # Capture the lock_id from the MERGE query to return it in the check
    def query_side_effect(
        query: str,
        job_config: bigquery.QueryJobConfig | None = None,
    ) -> mock.Mock:
        if "MERGE" in query and job_config:  # pragma: no branch
            # Extract the lock_id from the query parameters
            for param in job_config.query_parameters:
                if param.name == "lock_id":
                    mock_check_row.lock_id = param.value
            return mock_merge_job
        if "SELECT lock_id" in query:
            return mock_check_job
        if "DELETE" in query and "lock_id = @lock_id" in query:
            return mock_delete_job
        return mock_cleanup_job

    mock_client.query.side_effect = query_side_effect

    # Test successful lock acquisition
    with manager.acquire_lock("test_job", retry_seconds=0):
        pass

    # Verify queries were called: MERGE, SELECT, DELETE, cleanup DELETE
    assert mock_client.query.call_count == 4


def test_acquire_lock_retry(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test lock retry mechanism."""
    manager, mock_client = subject

    # Track call count to simulate retry behavior
    call_count = 0
    our_lock_id = None

    mock_merge_job = mock.Mock()
    mock_merge_job.result.return_value = []

    # First check returns someone else's lock_id, second returns ours
    mock_check_row_other = mock.Mock(lock_id="other-process-lock")
    mock_check_job_other = mock.Mock()
    mock_check_job_other.result.return_value = [mock_check_row_other]

    mock_check_row_ours = mock.Mock()
    mock_check_job_ours = mock.Mock()
    mock_check_job_ours.result.return_value = [mock_check_row_ours]

    mock_delete_job = mock.Mock()
    mock_delete_job.result.return_value = []

    mock_cleanup_job = mock.Mock()
    mock_cleanup_job.result.return_value = []

    def query_side_effect(
        query: str,
        job_config: bigquery.QueryJobConfig | None = None,
    ) -> mock.Mock:
        nonlocal call_count, our_lock_id
        call_count += 1

        if "MERGE" in query and job_config:  # pragma: no branch
            # Capture our lock_id from the query parameters
            for param in job_config.query_parameters:
                if param.name == "lock_id":
                    our_lock_id = param.value
                    mock_check_row_ours.lock_id = param.value
            return mock_merge_job
        if "SELECT lock_id" in query:
            # First check returns other's lock, second returns ours
            if call_count <= 2:
                return mock_check_job_other
            return mock_check_job_ours
        if "DELETE" in query and "lock_id = @lock_id" in query:
            return mock_delete_job
        return mock_cleanup_job

    mock_client.query.side_effect = query_side_effect

    # Test lock retry
    with manager.acquire_lock("test_job", retry_seconds=0.01):  # type: ignore[arg-type]
        pass

    # Verify it retried: MERGE, SELECT (other's), MERGE, SELECT (ours), DELETE, cleanup
    assert mock_client.query.call_count == 6


def test_missing_project_validation() -> None:
    """Test missing project validation."""
    with pytest.raises(
        MissingStateBackendSettingsError,
        match="BigQuery project is required",
    ):
        BigQueryStateStoreManager(
            uri="bigquery:///dataset",  # No project in hostname
            dataset="dataset",
        )


def test_missing_dataset_validation() -> None:
    """Test missing dataset validation."""
    with pytest.raises(
        MissingStateBackendSettingsError,
        match="BigQuery dataset is required",
    ):
        BigQueryStateStoreManager(
            uri="bigquery://project/",  # No dataset in path
            project="project",
        )


def test_acquire_lock_max_retries_exceeded(
    subject: tuple[BigQueryStateStoreManager, mock.Mock],
) -> None:
    """Test lock acquisition with max retries exceeded."""
    manager, mock_client = subject

    # Mock query results - MERGE succeeds but someone else always owns the lock
    mock_merge_job = mock.Mock()
    mock_merge_job.result.return_value = []

    mock_check_row = mock.Mock(lock_id="other-process-lock")
    mock_check_job = mock.Mock()
    mock_check_job.result.return_value = [mock_check_row]

    def query_side_effect(
        query: str,
        job_config: bigquery.QueryJobConfig | None = None,  # noqa: ARG001
    ) -> mock.Mock:
        if "MERGE" in query:
            return mock_merge_job
        if "SELECT lock_id" in query:
            return mock_check_job
        return mock.Mock(result=mock.Mock(return_value=[]))  # pragma: no cover

    mock_client.query.side_effect = query_side_effect

    retry_seconds = Decimal("0.01")

    # Mock sleep
    with (
        mock.patch("meltano_state_backend_bigquery.backend.sleep") as mock_sleep,
        pytest.raises(
            StateIDLockedError,
            match="Could not acquire lock for state_id: test_job",
        ),
        manager.acquire_lock("test_job", retry_seconds=retry_seconds),  # type: ignore[arg-type]
    ):
        pass  # pragma: no cover

    # Each retry does MERGE + SELECT, so sleep count is (30 / 0.01) - 1 = 2999
    assert mock_sleep.call_count == int(30 / retry_seconds) - 1


def test_client_with_credentials_path() -> None:
    """Test client creation with credentials path."""
    with (
        mock.patch("google.cloud.bigquery.Client.from_service_account_json") as mock_from_json,
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_dataset",
        ),
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_tables",
        ),
    ):
        mock_client = mock.Mock()
        mock_from_json.return_value = mock_client

        manager = BigQueryStateStoreManager(
            uri="bigquery://testproject/testdataset",
            project="testproject",
            dataset="testdataset",
            credentials_path="/path/to/credentials.json",
        )

        # Access the client property to trigger creation
        _ = manager.client

        # Verify credentials path was used
        mock_from_json.assert_called_once_with(
            "/path/to/credentials.json",
            project="testproject",
        )


def test_client_without_credentials_path() -> None:
    """Test client creation without credentials path (uses default credentials)."""
    with (
        mock.patch("google.cloud.bigquery.Client") as mock_client_class,
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_dataset",
        ),
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_tables",
        ),
    ):
        mock_client = mock.Mock()
        mock_client_class.return_value = mock_client

        manager = BigQueryStateStoreManager(
            uri="bigquery://testproject/testdataset",
            project="testproject",
            dataset="testdataset",
            # No credentials_path
        )

        # Access the client property to trigger creation
        _ = manager.client

        # Verify default client was used (not from_service_account_json)
        mock_client_class.assert_called_once_with(project="testproject")


def test_ensure_dataset_exists() -> None:
    """Test _ensure_dataset when dataset already exists."""
    with (
        mock.patch("google.cloud.bigquery.Client") as mock_client_class,
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_tables",
        ),
    ):
        mock_client = mock.Mock()
        mock_client_class.return_value = mock_client
        # Dataset exists - get_dataset succeeds
        mock_client.get_dataset.return_value = mock.Mock()

        manager = BigQueryStateStoreManager(
            uri="bigquery://testproject/testdataset",
            project="testproject",
            dataset="testdataset",
        )

        # Verify get_dataset was called but create_dataset was not
        mock_client.get_dataset.assert_called_once_with("testproject.testdataset")
        mock_client.create_dataset.assert_not_called()
        assert manager.dataset == "testdataset"


def test_ensure_dataset_creates_when_not_found() -> None:
    """Test _ensure_dataset creates dataset when it doesn't exist."""
    from google.cloud.exceptions import NotFound

    with (
        mock.patch("google.cloud.bigquery.Client") as mock_client_class,
        mock.patch("google.cloud.bigquery.Dataset") as mock_dataset_class,
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_tables",
        ),
    ):
        mock_client = mock.Mock()
        mock_client_class.return_value = mock_client
        # Dataset doesn't exist - get_dataset raises NotFound
        mock_client.get_dataset.side_effect = NotFound("Dataset not found")  # type: ignore[no-untyped-call]

        mock_dataset = mock.Mock()
        mock_dataset_class.return_value = mock_dataset

        manager = BigQueryStateStoreManager(
            uri="bigquery://testproject/testdataset",
            project="testproject",
            dataset="testdataset",
            location="EU",
        )

        # Verify dataset was created
        mock_dataset_class.assert_called_once_with("testproject.testdataset")
        assert mock_dataset.location == "EU"
        mock_client.create_dataset.assert_called_once_with(mock_dataset)
        assert manager.dataset == "testdataset"


def test_ensure_tables_exist() -> None:
    """Test _ensure_tables when tables already exist."""
    with (
        mock.patch("google.cloud.bigquery.Client") as mock_client_class,
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_dataset",
        ),
    ):
        mock_client = mock.Mock()
        mock_client_class.return_value = mock_client
        # Tables exist - get_table succeeds
        mock_client.get_table.return_value = mock.Mock()

        manager = BigQueryStateStoreManager(
            uri="bigquery://testproject/testdataset",
            project="testproject",
            dataset="testdataset",
        )

        # Verify get_table was called for both tables but create_table was not
        assert mock_client.get_table.call_count == 2
        mock_client.get_table.assert_any_call("testproject.testdataset.meltano_state")
        mock_client.get_table.assert_any_call("testproject.testdataset.meltano_state_locks")
        mock_client.create_table.assert_not_called()
        assert manager.table_name == "meltano_state"


def test_ensure_tables_creates_when_not_found() -> None:
    """Test _ensure_tables creates tables when they don't exist."""
    from google.cloud.exceptions import NotFound

    with (
        mock.patch("google.cloud.bigquery.Client") as mock_client_class,
        mock.patch("google.cloud.bigquery.Table") as mock_table_class,
        mock.patch("google.cloud.bigquery.SchemaField"),
        mock.patch(
            "meltano_state_backend_bigquery.backend.BigQueryStateStoreManager._ensure_dataset",
        ),
    ):
        mock_client = mock.Mock()
        mock_client_class.return_value = mock_client
        # Tables don't exist - get_table raises NotFound
        mock_client.get_table.side_effect = NotFound("Table not found")  # type: ignore[no-untyped-call]

        mock_table = mock.Mock()
        mock_table_class.return_value = mock_table

        manager = BigQueryStateStoreManager(
            uri="bigquery://testproject/testdataset",
            project="testproject",
            dataset="testdataset",
        )

        # Verify tables were created
        assert mock_table_class.call_count == 2
        assert mock_client.create_table.call_count == 2
        assert manager.table_name == "meltano_state"
