FROM python:3.10-slim-bullseye

LABEL creator="AQAI" \
    maintainer="Christina Last, Prithviraj Prahmanik" \
    openaq-engine.version="development"


RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc build-essential libpq-dev liblapack-dev postgresql git

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends gnupg2 wget && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo "deb http://apt.postgresql.org/pub/repos/apt/ buster-pgdg main" | tee  /etc/apt/sources.list.d/pgdg.list && \
    apt-get update -y && \
    apt-get install -y --no-install-recommends postgresql-client-12

ARG ssh_prv_key
ARG ssh_pub_key

RUN apt-get update && \
    apt-get install -y \
    git \
    openssh-server \
    default-libmysqlclient-dev

# Authorize SSH Host
RUN mkdir -p /root/.ssh && \
    chmod 0700 /root/.ssh && \
    ssh-keyscan github.com > /root/.ssh/known_hosts

# Add the keys and set permissions
RUN echo "$ssh_prv_key" > /root/.ssh/id_rsa && \
    echo "$ssh_pub_key" > /root/.ssh/id_rsa.pub && \
    chmod 600 /root/.ssh/id_rsa && \
    chmod 600 /root/.ssh/id_rsa.pub

ENV SHELL=/bin/bash
ENV USERNAME=openaq_engine

RUN adduser \
    --disabled-password \
    --gecos "" \
    "${USERNAME}"

RUN mkdir -p /opt/venv
RUN chown -R ${USERNAME}:${USERNAME} /opt/venv
RUN chown -R ${USERNAME}:${USERNAME} .

USER ${USERNAME}

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR openaq-engine

RUN pip install poetry
# RUN pip install "poetry==$POETRY_VERSION"
COPY poetry.lock pyproject.toml /app/
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-ansi

ENTRYPOINT [ "bash" ]