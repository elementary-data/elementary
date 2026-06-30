# `edr artifacts`

Query Elementary's warehouse artifact tables from the CLI for agent and script consumption.

Every subcommand emits a JSON envelope on stdout by default. Logs and warnings go to stderr, so `edr artifacts ... | jq` always works without filtering. Errors are written to stderr as JSON with a stable `{error, code, details}` shape. Exit code `0` on success, `1` on user error (bad arguments, not found), `2` on system error (connection, unexpected).

## Global options

All commands accept:

| Flag | Purpose |
| --- | --- |
| `-o, --output {json,table}` | Output format. `json` (default) for agents; `table` for humans. |
| `--profile NAME` | Override the profile in `profiles.yml` (default `elementary`). |
| `-t, --profile-target NAME` | Target to load from the selected profile. |
| `-p, --profiles-dir PATH` | Directory containing `profiles.yml`. Defaults to CWD then `~/.dbt/`. |
| `--project-dir PATH` | Directory containing `dbt_project.yml`. Defaults to CWD. |
| `-c, --config-dir PATH` | Directory containing edr's `config.yml`. |
| `--target-path PATH` | Where edr writes its logs. |

List commands also accept `--limit N` (1–1000, default 200) and return `has_more: true` when the page is truncated. Fetch the next page by narrowing filters — cursors are not supported.

## JSON envelopes

List responses:

```json
{"count": 10, "has_more": false, "<entity_plural>": [...], "data": {"length": 10}}
```

Single-get responses:

```json
{"<entity_singular>": {...}}
```

Error responses (stderr):

```json
{"error": "Test test.x.y.z not found.", "code": "NOT_FOUND", "details": {"unique_id": "test.x.y.z"}}
```

## Commands

### Test results

- `edr artifacts test-results` — list test execution results.
- `edr artifacts test-result <test_execution_id>` — fetch a single test result.

Filters: `--test-unique-id`, `--model-unique-id`, `--test-type`, `--test-sub-type`, `--test-name` (LIKE), `--status`, `--table-name` (LIKE), `--column-name`, `--database-name`, `--schema-name`, `--severity`, `--detected-after`, `--detected-before`.

### Run results (dbt model execution)

- `edr artifacts run-results` — list model run results.
- `edr artifacts run-result <model_execution_id>` — fetch a single run result.

Filters: `--unique-id`, `--invocation-id`, `--status`, `--resource-type`, `--materialization`, `--name` (LIKE), `--started-after`, `--started-before`, `--execution-time-gt`, `--execution-time-lt`, `--include-compiled-code/--no-include-compiled-code` (default: exclude).

### Invocations

- `edr artifacts invocations` — list dbt invocations (default window: last 7 days).
- `edr artifacts invocation <invocation_id>` — fetch a single invocation.

Filters: `--invocation-id` (repeatable), `--command`, `--project-name`, `--orchestrator`, `--job-id`, `--job-run-id`, `--target-name`, `--target-schema`, `--target-profile-name`, `--full-refresh/--no-full-refresh`, `--started-after`, `--started-before`.

### Models

- `edr artifacts models` — list dbt model definitions.
- `edr artifacts model <unique_id>` — fetch a single model definition.

Filters: `--database-name`, `--schema-name`, `--materialization`, `--name` (LIKE over name/alias), `--package-name`, `--group-name`, `--generated-after`, `--generated-before`.

### Sources

- `edr artifacts sources` — list dbt source definitions.
- `edr artifacts source <unique_id>` — fetch a single source.

Filters: `--database-name`, `--schema-name`, `--source-name`, `--name` (LIKE on table name), `--identifier`, `--package-name`, `--generated-after`, `--generated-before`.

### Tests

- `edr artifacts tests` — list dbt test definitions.
- `edr artifacts test <unique_id>` — fetch a single test definition.

Filters: `--database-name`, `--schema-name`, `--name` (LIKE over name/short_name/alias), `--package-name`, `--test-type {generic,singular,expectation}`, `--test-namespace`, `--severity {warn,error}`, `--parent-model-unique-id`, `--quality-dimension`, `--group-name`, `--generated-after`, `--generated-before`.

## Examples

```bash
# Recent failed tests as JSON
edr artifacts test-results --status fail --detected-after 2026-01-01 --limit 50

# One invocation, human-readable
edr artifacts invocation 4a0b... -o table

# Models in a schema, filtered by name
edr artifacts models --schema-name analytics --name orders

# Tests covering a specific model
edr artifacts tests --parent-model-unique-id model.my_pkg.orders
```
