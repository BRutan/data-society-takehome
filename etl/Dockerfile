FROM python:3.12-slim

COPY ./etl/requirements.txt .

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     openjdk-21-jre ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Make a non root user for etls:
RUN useradd -m -u 10001 etl-user

# Install python:
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Show the spark-ui:
USER etl-user

ARG DB_HOST
ARG DB_USER
ARG DB_PASSWORD
ARG DB_DBNAME
ARG DB_PORT

WORKDIR ${HOME}

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
ENV PIP_NO_CACHE_DIR=1
ENV PYTHONPATH=${HOME}
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV DB_HOST=${DB_HOST}
ENV DB_USER=${DB_USER}
ENV DB_PASSWORD=${DB_PASSWORD}
ENV DB_DBNAME=${DB_DBNAME}
ENV DB_PORT=${DB_PORT}

COPY ./etl/config/ ./config/
COPY ./etl/jobs/ ./jobs/
COPY ./etl/shared/ ./shared/