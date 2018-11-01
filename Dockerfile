FROM mcarans/hdxscraper-docker-base

MAINTAINER Michael Rans <rans@email.com>

RUN apk add --no-cache --upgrade git && \
    cd /root && \
    git clone https://github.com/OCHA-DAP/hdxscraper-fts && \
    apk del git && \
    rm -rf /var/lib/apk/*

CMD ["python", "/root/hdxscraper-fts/run.py"
