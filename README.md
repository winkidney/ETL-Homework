# ETL-Homework

![Tests Status](https://github.com/winkidney/ETL-Homework/actions/workflows/py-tests.yml/badge.svg)

# Development

Install requires:
```shell
# poetry is required for development and python>=3.9 is required
pip install poetry
# install env
poetry install

# copy git hook
cp ./scripts/pre-commit.example ./git/hooks/pre-commit
```

Test:
```shell
make test
```

Run on cloud:
```shell
# switch to cloud and login with API-KEY or browser
# note: you should first register an account at https://www.prefect.io/
make switch2cloud
# then run task-entry
make local-task
```

# Deploy
Now we only use local process to serve `depolyment` instance since we have limited calculation and data fetching.

just run:

```shell
make local-task
```

It will make the deployment done.
