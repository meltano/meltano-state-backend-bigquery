# `meltano-state-backend-bigquery`

<!-- Display these if and when we publish to PyPI. -->

<!--
[![PyPI version](https://img.shields.io/pypi/v/meltano-state-backend-bigquery.svg?logo=pypi&logoColor=FFE873&color=blue)](https://pypi.org/project/meltano-state-backend-bigquery)
[![Python versions](https://img.shields.io/pypi/pyversions/meltano-state-backend-bigquery.svg?logo=python&logoColor=FFE873)](https://pypi.org/project/meltano-state-backend-bigquery) -->

This is a [Meltano] extension that provides a [BigQuery] [state backend][state-backend].

## Installation

This package needs to be installed in the same Python environment as Meltano.

### From GitHub

#### With [uv]

```bash
uv tool install --with git+https://github.com/meltano/meltano-state-backend-bigquery.git meltano
```

#### With [pipx]

```bash
pipx install meltano
pipx inject meltano git+https://github.com/meltano/meltano-state-backend-bigquery.git
```

## Configuration

To store state in BigQuery, set the `state_backend.uri` setting to `bigquery://<project>/<dataset>`.

State will be stored in two tables that Meltano will create automatically:

- `meltano_state` - Stores the actual state data
- `meltano_state_locks` - Manages concurrency locks

To authenticate to BigQuery, you can use either:

1. Application Default Credentials (recommended for GCP environments)
1. Service account JSON key file

### Using Application Default Credentials

```yaml
state_backend:
  uri: bigquery://my-project/my-dataset
  bigquery:
    project: my-project
    dataset: my-dataset
    location: US  # Optional: defaults to US
```

### Using Service Account JSON Key

```yaml
state_backend:
  uri: bigquery://my-project/my-dataset
  bigquery:
    project: my-project
    dataset: my-dataset
    location: US  # Optional: defaults to US
    credentials_path: /path/to/service-account-key.json
```

#### Connection Parameters

- **project**: Your GCP project ID (required)
- **dataset**: The BigQuery dataset where state will be stored (required)
- **location**: The BigQuery dataset location (optional, defaults to US)
- **credentials_path**: Path to service account JSON key file (optional, uses Application Default Credentials if not specified)

#### Security Considerations

When storing credentials:

- Use environment variables for sensitive values in production
- Consider using Application Default Credentials when running on GCP
- Ensure the service account has the following BigQuery permissions:
  - `bigquery.datasets.create` (if dataset doesn't exist)
  - `bigquery.tables.create` (if tables don't exist)
  - `bigquery.tables.get`
  - `bigquery.tables.updateData`
  - `bigquery.tables.getData`

Example using environment variables:

```bash
export MELTANO_STATE_BACKEND_BIGQUERY_CREDENTIALS_PATH='/path/to/key.json'
meltano config set meltano state_backend.uri 'bigquery://my-project/my-dataset'
```

## Development

### Setup

```bash
uv sync
```

### Run tests

Run all tests, type checks, linting, and coverage:

```bash
uvx --with tox-uv tox run-parallel
```

### Bump the version

Using the [GitHub CLI][gh]:

```bash
gh release create v<new-version>
```

[bigquery]: https://cloud.google.com/bigquery
[gh]: https://cli.github.com/
[meltano]: https://meltano.com
[pipx]: https://github.com/pypa/pipx
[state-backend]: https://docs.meltano.com/concepts/state_backends
[uv]: https://docs.astral.sh/uv
