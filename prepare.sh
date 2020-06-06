#!/bin/bash
set -e

BUILD_THIRD_PARTY_PATH="build/third_party"

function print_third_party_msg() {
  echo -e "\e[94m====== Preparing the third party '$1' ======\e[0m"
}

# TODO Add test checker
function prepare_third_party() {
  print_third_party_msg "$1"
  (
    cd "$BUILD_THIRD_PARTY_PATH/$1"
    mkdir build -p
    cd build
    if ! $ONLY_INSTALL ; then
      cmake .. -DCMAKE_BUILD_TYPE=Release "${@:2}"
      make "-j${MAKE_N_PROC}"
    fi
    sudo make install

    sudo ldconfig
  )
}

function prepare_protobuf() {
  print_third_party_msg protobuf
  (
    cd "$BUILD_THIRD_PARTY_PATH/grpc/third_party/protobuf"
    if ! $ONLY_INSTALL ; then
      ./autogen.sh
      ./configure
      make "-j${MAKE_N_PROC}"
    fi
    sudo make install

    sudo ldconfig
  )
}

cd "/source"

ONLY_INSTALL=false
MAKE_N_PROC=8

POSITIONAL=()
while [[ $# -gt 0 ]]
do
  key="$1"

  case $key in
      --no-proc)
      MAKE_N_PROC="$2"
      shift
      shift
      ;;
      --only-install)
      ONLY_INSTALL=true
      shift
      ;;
      *)    # unknown option
      POSITIONAL+=("$1") # save it in an array for later
      shift # past argument
      ;;
  esac
done

set -- "${POSITIONAL[@]}" # restore positional parameters

set -x

if ! $ONLY_INSTALL ; then
  #rm -Rf build
  mkdir -p build
  rsync -a third_party build --exclude .git
fi

prepare_protobuf
#TODO probably this isn't neccesary because it was loaded in the cmakelist, take a look on it
prepare_third_party abseil-cpp
prepare_third_party grpc "-DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF"
prepare_third_party inja
prepare_third_party json
prepare_third_party catch2
prepare_third_party fmt
prepare_third_party spdlog
prepare_third_party prometheus-cpp
prepare_third_party yaml-cpp "-DYAML_CPP_BUILD_TESTS=OFF -DYAML_CPP_BUILD_TOOLS=OFF -DYAML_CPP_BUILD_CONTRIB=OFF"
