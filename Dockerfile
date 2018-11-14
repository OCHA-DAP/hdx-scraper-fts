FROM unocha/hdx-scraper-baseimage:stable

WORKDIR /srv

COPY . .

RUN apk add --update-cache --virtual .build-deps \
        build-base \
        python3-dev \
        openssl-dev && \
    pip install -r requirements.txt && \
    apk del .build-deps && \
    rm -rf /var/cache/apk/*

CMD ["python3", "run.py"]
