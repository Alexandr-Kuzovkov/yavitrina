FROM python:2.7.12

RUN \
apt-get update && \
apt-get install -y software-properties-common apt-utils python-software-properties && \
apt-get install -y  python-dev libxml2-dev python-pip zlib1g-dev libffi-dev libssl-dev libxslt1-dev \
net-tools tidy

RUN apt-get install -y mc

RUN mkdir /home/root

WORKDIR /home/root

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install pip install queuelib==1.5.0
COPY ./start.sh /usr/local/bin/start.sh

RUN chmod +x /usr/local/bin/start.sh

RUN mkdir /etc/scrapyd
RUN pip install scrapyd-client
RUN sed -i "s/scrapy.utils.http/w3lib.http/g" /usr/local/bin/scrapyd-deploy
RUN pip install alembic
CMD ["/usr/local/bin/start.sh"]
