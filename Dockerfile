FROM opencadc/astropy:3.9-slim

RUN apt-get update --no-install-recommends && \
    apt-get install -y \
        build-essential \
        git && \
    rm -rf /var/lib/apt/lists/ /tmp/* /var/tmp/*
    
RUN pip install cadcdata \
    cadctap \
    caom2 \
    caom2repo \
    caom2utils \
    importlib-metadata \
    python-dateutil \
    PyYAML \
    spherical-geometry \
    vos

RUN oldpath=`pwd` && cd /tmp && \
    wget http://www.eso.org/~fstoehr/footprintfinder.py && \
    cp footprintfinder.py /usr/local/lib/python3.9/site-packages/footprintfinder.py && \
    chmod 755 /usr/local/lib/python3.9/site-packages/footprintfinder.py && \
    cd $oldpath

WORKDIR /usr/src/app

ARG CAOM2_BRANCH=master
ARG CAOM2_REPO=opencadc
ARG OPENCADC_BRANCH=master
ARG OPENCADC_REPO=opencadc
ARG PIPE_BRANCH=master
ARG PIPE_REPO=opencadc
ARG VOS_BRANCH=master
ARG VOS_REPO=opencadc

RUN git clone https://github.com/opencadc/cadctools.git && \
    cd cadctools && \
    pip install ./cadcdata && \
    cd ..

RUN git clone https://github.com/${VOS_REPO}/vostools.git && \
    cd vostools && \
    git checkout ${VOS_BRANCH} && \
    pip install ./vos && \
    cd ..

RUN git clone https://github.com/${CAOM2_REPO}/caom2tools.git && \
    cd caom2tools && \
    git checkout ${CAOM2_BRANCH} && \
    pip install ./caom2utils && \
    cd ..

RUN pip install git+https://github.com/${OPENCADC_REPO}/caom2pipe@${OPENCADC_BRANCH}#egg=caom2pipe
  
RUN pip install git+https://github.com/${OPENCADC_REPO}/wallaby2caom2@${OPENCADC_BRANCH}#egg=wallaby2caom2

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
