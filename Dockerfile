FROM opencadc/astropy:3.9-slim

RUN apt-get update -y && apt-get dist-upgrade -y && \
    apt-get install -y \
        build-essential \
        git && \
    rm -rf /var/lib/apt/lists/ /tmp/* /var/tmp/*
    
RUN pip install cadcdata \
    cadctap \
    caom2 \
    caom2repo \
    caom2utils \
    ftputil \
    importlib-metadata \
    PyYAML \
    spherical-geometry \
    vos

WORKDIR /usr/src/app

ARG OPENCADC_REPO=opencadc
ARG OPENCADC_BRANCH=master

RUN pip install git+https://github.com/${OPENCADC_REPO}/caom2pipe@${OPENCADC_BRANCH}#egg=caom2pipe
  
RUN pip install git+https://github.com/${OPENCADC_REPO}/wallaby2caom2@${OPENCADC_BRANCH}#egg=wallaby2caom2

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
