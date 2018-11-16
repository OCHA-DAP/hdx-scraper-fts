FROM unocha/hdx-scraper-baseimage:stable

WORKDIR /srv

COPY . .

RUN pip install -r requirements.txt

CMD ["python3", "run.py"]
