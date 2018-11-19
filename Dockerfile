FROM unocha/hdx-scraper-baseimage:stable

WORKDIR /srv

COPY . .

RUN mkdir /srv/tmp

CMD ["python3", "run.py"]
