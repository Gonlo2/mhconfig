FROM gcc:8.2
MAINTAINER Juan Manuel Fdez

### base ###
RUN apt-get update && apt-get install -y --no-install-recommends \
  cmake \
  automake \
  bison \
  curl \
  flex \
  g++ \
  libboost-all-dev \
  libssl-dev \
  libtool \
  make \
  pkg-config \
  libomp-dev \
  valgrind \
  rsync \
  tcpdump \
  vim \
  sudo \
  ccache \
  python-pip && \
  pip install --upgrade setuptools && \
  pip install conan

ARG VALGRIND_VERSION=3.13.0

RUN cd /tmp && \
  wget "ftp://sourceware.org/pub/valgrind/valgrind-${VALGRIND_VERSION}.tar.bz2" && \
  tar xjf "valgrind-${VALGRIND_VERSION}.tar.bz2" && \
  cd "valgrind-${VALGRIND_VERSION}" && \
  ./configure && \
  make -j4 && \
  make install && \
  cd .. && \
  rm "valgrind-${VALGRIND_VERSION}"* -f -R

CMD sleep infinity
