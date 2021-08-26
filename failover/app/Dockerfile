FROM python:3.6.2

RUN mkdir /app && mkdir /data

WORKDIR /app

RUN \
apt-get update && \
apt-get install -y software-properties-common apt-utils python-software-properties && \
apt-get install -y  python-dev libxml2-dev libxslt1-dev antiword unrtf poppler-utils pstotext tesseract-ocr flac \
lame libmad0 libsox-fmt-mp3 sox libjpeg-dev swig libpulse-dev tar gzip wget net-tools telnet

RUN add-apt-repository "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" && \
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
apt-get update && \
apt-get install -y postgresql-9.6 && \
apt-get install -y mysql-client netcat-openbsd

COPY run /usr/local/bin/
COPY checkjobs /usr/local/bin/
RUN chmod +x /usr/local/bin/run && chmod +x /usr/local/bin/checkjobs
COPY requirements.txt /app/
COPY scripts /app/scripts
COPY config.ini /app/config.ini
COPY start-web.sh /app/start-web.sh
RUN chmod +x /app/scripts/run.py && chmod +x /app/start-web.sh
#RUN pip install --upgrade pip
RUN pip install --upgrade pip==18.1
RUN pip install --no-cache-dir -r requirements.txt && pip3 install --upgrade pip
COPY README.txt /app/README.txt
COPY help.txt /app/help.txt

CMD ["/app/start-web.sh"]

