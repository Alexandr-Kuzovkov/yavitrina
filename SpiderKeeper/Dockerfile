FROM python:2.7.12


WORKDIR /home/root

RUN \
apt-get update && \
apt-get install -y software-properties-common apt-utils python-software-properties && \
apt-get install -y  python-dev libxml2-dev python-pip zlib1g-dev libffi-dev libssl-dev libxslt1-dev \
poppler-utils pstotext tesseract-ocr libmad0 libjpeg-dev swig git python3 python3-pip net-tools

RUN apt-get install -y mc

RUN mkdir /scrapy

#COPY ./jobscrapers /scrapy/jobscrapers
#COPY ./jobimporters /scrapy/jobimporters
#COPY ./feedgenerator /scrapy/feedgenerator
#COPY ./monitor /scrapy/monitor

COPY ./start.sh /usr/src/app/start.sh
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY setup.py /scrapy/
COPY MANIFEST.in /scrapy/
COPY README.md /scrapy/
COPY __init__.py /scrapy/
COPY ./SpiderKeeper /scrapy/SpiderKeeper
RUN cd /scrapy && python setup.py install

RUN chmod +x /usr/src/app/start.sh
CMD ["/usr/src/app/start.sh"]

