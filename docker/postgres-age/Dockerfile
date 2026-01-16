# docker/postgres-age/Dockerfile
# PostgreSQL 16 with Apache AGE extension
#
# Uses official Apache AGE Docker image instead of building from source
# See: https://hub.docker.com/r/apache/age
# See: https://age.apache.org/blog/2024-05-21-how-to-use-the-official-apache-age-docker-image/

FROM apache/age:release_PG16_1.6.0

# Copy initialization script to enable AGE extension
COPY docker-entrypoint-initdb.d/ /docker-entrypoint-initdb.d/
