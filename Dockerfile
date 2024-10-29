FROM python:3.9-buster
RUN mkdir /projects/
WORKDIR /projects/

COPY ./poetry.lock /projects/
COPY ./pyproject.toml /projects/

RUN pip install poetry && poetry install
CMD ["/bin/bash", "-c", "-l"]
