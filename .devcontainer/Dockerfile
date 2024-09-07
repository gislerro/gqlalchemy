FROM python:3.12-alpine

# Update the package list and install packages
RUN apk update && apk add build-base cmake libffi-dev libressl-dev musl-dev linux-headers wget curl git vim

# Install bash
RUN apk add bash
RUN ln -sf /bin/bash /bin/sh

# Install pipx
RUN python3 -m ensurepip --upgrade
RUN python3 -m pip install pipx
ENV PIPX_HOME=/opt/pipx
ENV PIPX_BIN_DIR=/usr/local/bin

RUN python3 -m pipx ensurepath --global

# Install poetry
RUN pipx install poetry
RUN poetry config virtualenvs.create false

# Install dependencies
COPY pyproject.toml /gqlalchemy-repo/pyproject.toml
COPY poetry.lock /gqlalchemy-repo/poetry.lock




