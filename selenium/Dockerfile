FROM ubuntu:xenial

RUN apt-get update; apt-get clean

# Add a user for running applications.
RUN useradd apps
RUN mkdir -p /home/apps && chown apps:apps /home/apps

# Install x11vnc.
RUN apt-get install -y x11vnc

# Install xvfb.
RUN apt-get install -y xvfb

# Install fluxbox.
RUN apt-get install -y fluxbox

# Install wget.
RUN apt-get install -y wget

# Install wmctrl.
RUN apt-get install -y wmctrl

# Install Chrome.
COPY ./bin/google-chrome-stable_95.0.4638.69-1_amd64.deb /tmp/google-chrome-stable.deb
RUN apt-get update && apt-get install -y /tmp/google-chrome-stable.deb \
  && rm /tmp/google-chrome-stable.deb

RUN apt-get install apt-transport-https ca-certificates
#install python, seleneum, firefox, etc
RUN apt-get update && apt-get install -y \
    python python-pip curl unzip libgconf-2-4
RUN pip install selenium

RUN apt-get update --fix-missing && apt-get install -y software-properties-common && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A6DCF7707EBC211F && \
    apt-add-repository "deb http://ppa.launchpad.net/ubuntu-mozilla-security/ppa/ubuntu xenial main" && \
    apt-get update && \
    apt-get install -y firefox nginx

RUN apt-get install -y sudo && \
    echo "apps ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/apps && \
    chmod 0440 /etc/sudoers.d/apps

COPY ./requirements.txt /
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get install -y libpq-dev
#RUN pip install redis psycopg2-binary datetime
RUN pip install redis datetime
RUN pip install terminaltables daemonize
#cron
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ `lsb_release -cs`-pgdg main" |sudo tee  /etc/apt/sources.list.d/pgdg.list

#https://sites.google.com/a/chromium.org/chromedriver/
#https://chromedriver.storage.googleapis.com/index.html
COPY ./bin/chromedriver /usr/local/bin/chromedriver
RUN chmod a+x /usr/local/bin/chromedriver
#RUN pip install --upgrade pip
RUN pip install tendo
COPY nginx.conf /etc/nginx
COPY bootstrap.sh /
COPY ./app /usr/src/app
COPY uwsgi.ini /usr/src/
RUN apt-get install -y python-flask
#RUN echo "deb https://deb.goaccess.io/ $(lsb_release -cs) main" | sudo tee -a /etc/apt/sources.list.d/goaccess.list
#RUN    wget -O - https://deb.goaccess.io/gnugpg.key | sudo apt-key --keyring /etc/apt/trusted.gpg.d/goaccess.gpg add -
#RUN apt-get install -y apt-transport-https ca-certificates
#RUN    sudo apt-get update
#RUN    sudo apt-get install -y goaccess

#RUN apt-get install -y apt-transport-https ca-certificates
#RUN wget -O - https://deb.goaccess.io/gnugpg.key | gpg --dearmor | sudo tee /usr/share/keyrings/goaccess.gpg >/dev/null
#RUN echo "deb [signed-by=/usr/share/keyrings/goaccess.gpg] https://deb.goaccess.io/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/goaccess.list
#RUN sudo apt-get update
RUN sudo apt-get install goaccess



CMD '/bootstrap.sh'