FROM ubuntu:14.04
MAINTAINER Diffeo <support@diffeo.com>

RUN apt-get update -y && apt-get install -y python python-pip

# Install C libraries needed for future Python installs
RUN env DEBIAN_FRONTEND=noninteractive \
    apt-get install --no-install-recommends --assume-yes \
        ant \
        curl \
        g++ \
        gcc \
        gfortran \
        git \
        golang-1.6 \
        libboost-python-dev \
        libblas-dev \
        libffi-dev \
        libfreetype6-dev \
        libjpeg-dev \
        liblapack-dev \
        liblzma-dev \
        libpcre3-dev \
        libpq-dev \
        libpng12-dev \
        libsnappy1 \
        libsnappy-dev \
        libssl-dev \
        libxml2-dev \
        libxslt1-dev \
        libz-dev \ 
        make \
        openjdk-7-jdk \
        pkg-config \
        python-dev \
        python-virtualenv \
        xz-utils \
        zlib1g-dev \
 && apt-get clean

# We could install these with `pip`, but this is so much faster.
RUN apt-get install -y \
      python-numpy python-scipy python-sklearn python-matplotlib \
      python-gevent uwsgi

# This is now handled below with dossier.nltk_download to get only what is needed
# Download and install the NLTK corpus.
# RUN pip install nltk \
#  && python -m nltk.downloader -d /usr/share/nltk_data all

# Upgrade pip.
RUN pip install --upgrade pip

RUN pip install --upgrade pika

# Install psycopg2 in case we want a postgres backend.
RUN pip install psycopg2

RUN pip install --upgrade scikit-learn

# doing this now makes it much faster to iterate on memex-dossier,
# because the layers up through the ADD statement below change much
# more slowly
COPY requirements.txt /
RUN pip install -r /requirements.txt

ADD memex-dossier.tar /opt/memex-dossier/
RUN cd /opt/memex-dossier \
    && pip install . \
    && memex_dossier.nltk_download

RUN cat /opt/memex-dossier/data/background-50000.tfidf.gz | gzip -d > /background-50000.tfidf
RUN cp /opt/memex-dossier/configs/config-example.yaml /config.yaml

RUN mkdir /var/log/diffeo && chmod a+rwX /var/log/diffeo

# Finally, run the memex_dossier.models web server.
ENV PROCESSES 4
EXPOSE 57314
CMD uwsgi \
      --http-socket 0.0.0.0:57314 \
      --wsgi memex_dossier.models.web.wsgi \
      --pyargv "-c /config.yaml" \
      --master \
      --processes $PROCESSES \
      --vacuum \
      --max-requests 5000
