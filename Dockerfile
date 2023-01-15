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

ENV SHELL=/bin/bash \
    USERNAME=openaq_engine \
    DB_NAME_OPENAQ=${DB_NAME_OPENAQ} \
    S3_OUTPUT_OPENAQ=${S3_OUTPUT_OPENAQ} \
    S3_BUCKET_OPENAQ=${S3_BUCKET_OPENAQ} \
    AWS_PROFILE=${AWS_PROFILE} \
    AWS_ACCESS_KEY=${AWS_ACCESS_KEY} \
    AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
    PGDATABASE=${PGDATABASE} \ 
    PGUSER=${PGUSER} \
    PGPASSWORD=${PGPASSWORD}

RUN adduser \
    --disabled-password \
    --gecos "" \
    "${USERNAME}"

RUN mkdir -p /opt/venv
RUN chown -R ${USERNAME}:${USERNAME} /opt/venv
# RUN chown -R ${USERNAME}:${USERNAME} .

RUN git clone git@github.com:AQ-AI/openaq-engine.git

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install poetry
# RUN pip install "poetry==$POETRY_VERSION"
WORKDIR /app/

RUN mkdir openaq_engine 
COPY poetry.lock pyproject.toml /app/
COPY openaq_engine/ /app/openaq_engine/
RUN chown -R ${USERNAME}:${USERNAME} /app/openaq_engine/
USER ${USERNAME}

RUN poetry install --no-interaction --no-ansi
ENTRYPOINT [ "bash" ]