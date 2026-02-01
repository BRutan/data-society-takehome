FROM postgres:16.0

RUN rm -r /docker-entrypoint-initdb.d/

# Copy ddls that get run on startup:
COPY ./db/objects.sql /docker-entrypoint-initdb.d/
