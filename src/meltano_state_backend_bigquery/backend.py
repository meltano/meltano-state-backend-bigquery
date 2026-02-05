"""StateStoreManager for BigQuery state backend."""

from __future__ import annotations

import json
from contextlib import contextmanager
from functools import cached_property
from time import sleep
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from meltano.core.error import MeltanoError
from meltano.core.setting_definition import SettingDefinition, SettingKind
from meltano.core.state_store.base import (
    MeltanoState,
    MissingStateBackendSettingsError,
    StateIDLockedError,
    StateStoreManager,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable


class BigQueryStateBackendError(MeltanoError):
    """Base error for BigQuery state backend."""


BIGQUERY_PROJECT = SettingDefinition(
    name="state_backend.bigquery.project",
    label="BigQuery Project",
    description="BigQuery project ID",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    env_specific=True,
)

BIGQUERY_DATASET = SettingDefinition(
    name="state_backend.bigquery.dataset",
    label="BigQuery Dataset",
    description="BigQuery dataset name",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    env_specific=True,
)

BIGQUERY_LOCATION = SettingDefinition(
    name="state_backend.bigquery.location",
    label="BigQuery Location",
    description="BigQuery dataset location (e.g., US, EU)",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    default="US",
    env_specific=True,
)

BIGQUERY_CREDENTIALS_PATH = SettingDefinition(
    name="state_backend.bigquery.credentials_path",
    label="BigQuery Credentials Path",
    description="Path to service account JSON key file",
    kind=SettingKind.STRING,  # ty: ignore[invalid-argument-type]
    sensitive=True,
    env_specific=True,
)


class BigQueryStateStoreManager(StateStoreManager):
    """State backend for BigQuery."""

    label: str = "BigQuery"
    table_name: str = "meltano_state"
    lock_table_name: str = "meltano_state_locks"

    def __init__(
        self,
        uri: str,
        *,
        project: str | None = None,
        dataset: str | None = None,
        location: str | None = None,
        credentials_path: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the BigQueryStateStoreManager.

        Args:
            uri: The state backend URI
            project: BigQuery project ID
            dataset: BigQuery dataset name
            location: BigQuery dataset location (default: US)
            credentials_path: Path to service account JSON key file
            kwargs: Additional keyword args to pass to parent

        """
        super().__init__(**kwargs)
        self.uri = uri
        parsed = urlparse(uri)

        # Extract connection details from URI and parameters
        self.project = project or parsed.hostname
        if not self.project:
            msg = "BigQuery project is required"
            raise MissingStateBackendSettingsError(msg)

        # Extract dataset from path
        path_parts = parsed.path.strip("/").split("/") if parsed.path else []
        self.dataset = dataset or (path_parts[0] if path_parts else None)
        if not self.dataset:
            msg = "BigQuery dataset is required"
            raise MissingStateBackendSettingsError(msg)

        self.location = location or "US"
        self.credentials_path = credentials_path

        self._ensure_dataset()
        self._ensure_tables()

    @cached_property
    def client(self) -> bigquery.Client:
        """Get a BigQuery client.

        Returns:
            A BigQuery client object.

        """
        if self.credentials_path:
            return bigquery.Client.from_service_account_json(  # type: ignore[no-any-return,no-untyped-call]
                self.credentials_path,
                project=self.project,
            )
        return bigquery.Client(project=self.project)

    def _ensure_dataset(self) -> None:
        """Ensure the dataset exists."""
        dataset_id = f"{self.project}.{self.dataset}"
        try:
            self.client.get_dataset(dataset_id)
        except NotFound:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = self.location
            self.client.create_dataset(dataset)

    def _ensure_tables(self) -> None:
        """Ensure the state and lock tables exist."""
        # Create state table
        state_table_id = f"{self.project}.{self.dataset}.{self.table_name}"
        state_schema = [
            bigquery.SchemaField("state_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("partial_state", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("completed_state", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ]

        try:
            self.client.get_table(state_table_id)
        except NotFound:
            state_table = bigquery.Table(state_table_id, schema=state_schema)
            self.client.create_table(state_table)

        # Create lock table
        lock_table_id = f"{self.project}.{self.dataset}.{self.lock_table_name}"
        lock_schema = [
            bigquery.SchemaField("state_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("locked_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("lock_id", "STRING", mode="NULLABLE"),
        ]

        try:
            self.client.get_table(lock_table_id)
        except NotFound:
            lock_table = bigquery.Table(lock_table_id, schema=lock_schema)
            self.client.create_table(lock_table)

    def set(self, state: MeltanoState) -> None:
        """Set the job state for the given state_id.

        Args:
            state: the state to set.

        """
        partial_json = json.dumps(state.partial_state) if state.partial_state else None
        completed_json = json.dumps(state.completed_state) if state.completed_state else None

        query = f"""
            MERGE `{self.project}.{self.dataset}.{self.table_name}` AS target
            USING (
                SELECT
                    @state_id AS state_id,
                    PARSE_JSON(@partial_state) AS partial_state,
                    PARSE_JSON(@completed_state) AS completed_state
            ) AS source
            ON target.state_id = source.state_id
            WHEN MATCHED THEN
                UPDATE SET
                    partial_state = source.partial_state,
                    completed_state = source.completed_state,
                    updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (state_id, partial_state, completed_state, updated_at)
                VALUES (source.state_id, source.partial_state, source.completed_state, CURRENT_TIMESTAMP())
        """  # noqa: E501, S608

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("state_id", "STRING", state.state_id),
                bigquery.ScalarQueryParameter("partial_state", "STRING", partial_json),
                bigquery.ScalarQueryParameter("completed_state", "STRING", completed_json),
            ],
        )

        query_job = self.client.query(query, job_config=job_config)
        query_job.result()  # Wait for the query to complete

    def get(self, state_id: str) -> MeltanoState | None:
        """Get the job state for the given state_id.

        Args:
            state_id: the name of the job to get state for.

        Returns:
            The current state for the given job

        """
        query = f"""
            SELECT partial_state, completed_state
            FROM `{self.project}.{self.dataset}.{self.table_name}`
            WHERE state_id = @state_id
        """  # noqa: S608

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("state_id", "STRING", state_id),
            ],
        )

        query_job = self.client.query(query, job_config=job_config)
        results = list(query_job.result())

        if not results:
            return None

        row = results[0]

        # BigQuery returns None for NULL JSON columns
        # but MeltanoState expects empty dicts
        partial_state = row.partial_state if row.partial_state is not None else {}
        completed_state = row.completed_state if row.completed_state is not None else {}

        return MeltanoState(
            state_id=state_id,
            partial_state=partial_state,
            completed_state=completed_state,
        )

    def delete(self, state_id: str) -> None:
        """Delete state for the given state_id.

        Args:
            state_id: the state_id to clear state for

        """
        query = f"""
            DELETE FROM `{self.project}.{self.dataset}.{self.table_name}`
            WHERE state_id = @state_id
        """  # noqa: S608

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("state_id", "STRING", state_id),
            ],
        )

        query_job = self.client.query(query, job_config=job_config)
        query_job.result()  # Wait for the query to complete

    def clear_all(self) -> int:
        """Clear all states.

        Returns:
            The number of states cleared from the store.

        """
        # Get count first
        count_query = f"""
            SELECT COUNT(*) as count
            FROM `{self.project}.{self.dataset}.{self.table_name}`
        """  # noqa: S608
        query_job = self.client.query(count_query)
        count = next(iter(query_job.result())).count

        # Delete all rows
        delete_query = f"""
            DELETE FROM `{self.project}.{self.dataset}.{self.table_name}`
            WHERE TRUE
        """  # noqa: S608
        query_job = self.client.query(delete_query)
        query_job.result()  # Wait for the query to complete

        return count  # type: ignore[no-any-return]

    def get_state_ids(self, pattern: str | None = None) -> Iterable[str]:
        """Get all state_ids available in this state store manager.

        Args:
            pattern: glob-style pattern to filter by

        Returns:
            An iterable of state_ids

        """
        if pattern and pattern != "*":
            # Convert glob pattern to SQL LIKE pattern
            sql_pattern = pattern.replace("*", "%").replace("?", "_")
            query = f"""
                SELECT state_id
                FROM `{self.project}.{self.dataset}.{self.table_name}`
                WHERE state_id LIKE @pattern
            """  # noqa: S608
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("pattern", "STRING", sql_pattern),
                ],
            )
        else:
            query = f"""
                SELECT state_id
                FROM `{self.project}.{self.dataset}.{self.table_name}`
            """  # noqa: S608
            job_config = None

        query_job = self.client.query(query, job_config=job_config)
        for row in query_job.result():
            yield row.state_id

    @contextmanager
    def acquire_lock(
        self,
        state_id: str,
        *,
        retry_seconds: int = 1,
    ) -> Generator[None, None, None]:
        """Acquire a lock for the given job's state.

        Args:
            state_id: the state_id to lock
            retry_seconds: the number of seconds to wait before retrying

        Yields:
            None

        Raises:
            StateIDLockedError: if the lock cannot be acquired

        """
        import uuid

        lock_id = str(uuid.uuid4())
        max_seconds = 30
        seconds_waited = 0

        while seconds_waited < max_seconds:  # pragma: no branch
            # MERGE will only insert if no row exists for this state_id
            merge_query = f"""
                MERGE `{self.project}.{self.dataset}.{self.lock_table_name}` AS target
                USING (SELECT @state_id AS state_id) AS source
                ON target.state_id = source.state_id
                WHEN NOT MATCHED THEN
                    INSERT (state_id, locked_at, lock_id)
                    VALUES (@state_id, CURRENT_TIMESTAMP(), @lock_id)
            """  # noqa: S608

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("state_id", "STRING", state_id),
                    bigquery.ScalarQueryParameter("lock_id", "STRING", lock_id),
                ],
            )
            query_job = self.client.query(merge_query, job_config=job_config)
            query_job.result()

            # Check if we own the lock (MERGE doesn't tell us if insert happened)
            check_query = f"""
                SELECT lock_id
                FROM `{self.project}.{self.dataset}.{self.lock_table_name}`
                WHERE state_id = @state_id
            """  # noqa: S608
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("state_id", "STRING", state_id),
                ],
            )
            query_job = self.client.query(check_query, job_config=job_config)
            result = next(iter(query_job.result()), None)

            if result and result.lock_id == lock_id:
                # We got the lock
                break

            # Someone else has the lock
            seconds_waited += retry_seconds
            if seconds_waited >= max_seconds:
                msg = f"Could not acquire lock for state_id: {state_id}"
                raise StateIDLockedError(msg)
            sleep(retry_seconds)

        try:
            yield
        finally:
            # Release the lock
            delete_query = f"""
                DELETE FROM `{self.project}.{self.dataset}.{self.lock_table_name}`
                WHERE state_id = @state_id AND lock_id = @lock_id
            """  # noqa: S608
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("state_id", "STRING", state_id),
                    bigquery.ScalarQueryParameter("lock_id", "STRING", lock_id),
                ],
            )
            query_job = self.client.query(delete_query, job_config=job_config)
            query_job.result()  # Wait for the query to complete

            # Clean up old locks (older than 5 minutes)
            cleanup_query = f"""
                DELETE FROM `{self.project}.{self.dataset}.{self.lock_table_name}`
                WHERE locked_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
            """  # noqa: S608
            query_job = self.client.query(cleanup_query)
            query_job.result()  # Wait for the query to complete
