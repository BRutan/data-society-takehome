FROM python:3.12-slim

COPY ./etl/requirements.txt .

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
    PYTHONPATH=${HOME}

WORKDIR ${HOME}
COPY ./etl/jobs/ ./jobs/
COPY ./etl/shared/ ./shared/