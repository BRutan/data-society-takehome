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

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=${HOME} \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64 \
    PATH="$JAVA_HOME/bin:$PATH"

WORKDIR ${HOME}
COPY ./etl/jobs/ ./jobs/
COPY ./etl/shared/ ./shared/