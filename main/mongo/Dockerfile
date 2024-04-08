FROM debian:bookworm

RUN set -x \
  && apt-get update \
  && apt-get install -y --no-install-recommends apt-transport-https ca-certificates curl;

#RUN curl -LO https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2004-x86_64-100.5.4.tgz \
#  && tar -zxvf mongodb-database-tools-ubuntu2004-x86_64-100.5.4.tgz \
#  && cp -r ./mongodb-database-tools-ubuntu2004-x86_64-100.5.4/bin/* /bin/ ;

COPY wal-g /bin/wal-g

ENTRYPOINT  [ "/bin/wal-g" ]