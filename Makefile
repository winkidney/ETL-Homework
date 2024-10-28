sync-db:
	poetry run python -m etl_homework.migrations.initial_data
rm-dbs:
	rm -fr *.sqlite
serve-api:
	poetry run prefect server start
switch2local:
	poetry run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
switch2cloud:
	poetry run prefect cloud login
local-task:
	poetry run python -m etl_homework.serve
functional-test:
	poetry run pytest tests/functional -vv
unittest:
	poetry run pytest tests/unittest -vv
test:
	make unittest
	make functional-test
lint:
	poetry run black --check --diff .
format:
	poetry run black .

# for end usage
send-full-report:
	poetry run python -m etl_homework.handy.send_test_email
register-email-block:
	poetry run prefect block register -m prefect_email
export-to-local:
	poetry run python -m etl_homework.handy.export_to_local_dir
