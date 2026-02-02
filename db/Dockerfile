FROM postgres:16.0

RUN rm -r /docker-entrypoint-initdb.d/

# Copy ddls that get run on startup:
COPY ./db/ddls/* /docker-entrypoint-initdb.d/
