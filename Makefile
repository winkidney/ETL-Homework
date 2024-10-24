serve-api:
	poetry run prefect server start
switch2local:
	poetry run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
switch2cloud:
	poetry run prefect cloud login
local-task:
	poetry run python -m etl_homework.serve
test:
	poetry run pytest tests -vv
lint:
	poetry run black --check --diff .
format:
	poetry run black .
