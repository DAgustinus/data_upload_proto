# FROM postgres:latest
# ENV POSTGRES_PASSWORD=pwd
# ENV POSTGRES_USER=admin
# ENV POSTGRES_DB=database

FROM postgres
ENV POSTGRES_PASSWORD docker
ENV POSTGRES_DB world
COPY create_fixtures.sql /docker-entrypoint-initdb.d/create_fixtures.sql
