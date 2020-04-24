FROM ubuntu

# Install required packages
RUN apt-get update
RUN apt-get install -y libev-dev libsqlite3-dev libmariadb-dev postgresql-client influxdb

# Install zenoh
COPY zenoh-ubuntu-latest /eclipse-zenoh
RUN chmod +x /eclipse-zenoh/bin/zenohd.exe

EXPOSE 7447/udp
EXPOSE 7447/tcp
EXPOSE 8000/tcp

ENTRYPOINT service influxdb start && exec /eclipse-zenoh/bin/zenohd.exe -vv