#!/bin/bash

# Copyright, Aleksey Konovkin (alkon2000@mail.ru)
# BSD license type

download=0
if [ "$1" == "1" ]; then
  download=1
fi
build_deps=0

DIR="$(pwd)"

VERSION="1.13.12"
PCRE_VERSION="8.39"
LUAJIT_VERSION="2.1.0-beta2"

SUFFIX="-amqp"

BASE_PREFIX="$DIR/build"
INSTALL_PREFIX="$DIR/install"

PCRE_PREFIX="$DIR/build/pcre-$PCRE_VERSION"
JIT_PREFIX="$DIR/build/deps/luajit"

export LUAJIT_INC="$JIT_PREFIX/usr/local/include/luajit-2.1"
export LUAJIT_LIB="$JIT_PREFIX/usr/local/lib"

export LD_LIBRARY_PATH="$JIT_PREFIX/lib"

function clean() {
  rm -rf install  2>/dev/null
  rm -rf $(ls -1d build/* 2>/dev/null | grep -v deps)    2>/dev/null
  if [ $download -eq 1 ]; then
    rm -rf download 2>/dev/null
  fi
}


if [ "$1" == "clean" ]; then
  clean
  exit 0
fi

function build_luajit() {
  echo "Build luajit"
  cd LuaJIT-$LUAJIT_VERSION
  make -j 8 > /dev/null
  r=$?
  if [ $r -ne 0 ]; then
    exit $r
  fi
  DESTDIR="$JIT_PREFIX" make install > /dev/null
  cd ..
}

function build_cJSON() {
  echo "Build cjson"
  cd lua-cjson
  PREFIX="$JIT_PREFIX/usr/local" make > /dev/null
  r=$?
  if [ $r -ne 0 ]; then
    exit $r
  fi
  cd ..
}

function build_debug() {
  cd nginx-$VERSION
  echo "Configuring debug nginx-$VERSION$SUFFIX"
  ./configure --prefix="$INSTALL_PREFIX/nginx-$VERSION$SUFFIX" \
              --with-pcre=$PCRE_PREFIX \
              --with-stream \
              --with-debug \
              --with-cc-opt="-O0" \
              --add-module=../ngx_devel_kit \
              --add-module=../lua-nginx-module \
              --add-module=../stream-lua-nginx-module \
              --add-module=../ngx_dynamic_upstream \
              --add-module=../ngx_dynamic_upstream_lua \
              --add-module=../ngx_dynamic_healthcheck > /dev/null 2>/dev/stderr

  r=$?
  if [ $r -ne 0 ]; then
    exit $r
  fi

  echo "Build debug nginx-$VERSION$SUFFIX"
  make -j 8 > /dev/null 2>/dev/stderr

  r=$?
  if [ $r -ne 0 ]; then
    exit $r
  fi
  make install > /dev/null

  mv "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/sbin/nginx" "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/sbin/nginx.debug"
  cd ..
}

function build_release() {
  cd nginx-$VERSION
  echo "Configuring release nginx-$VERSION$SUFFIX"
  ./configure --prefix="$INSTALL_PREFIX/nginx-$VERSION$SUFFIX" \
              --with-pcre=$PCRE_PREFIX \
              --with-stream \
              --add-module=../ngx_devel_kit \
              --add-module=../lua-nginx-module \
              --add-module=../stream-lua-nginx-module \
              --add-module=../ngx_dynamic_upstream \
              --add-module=../ngx_dynamic_upstream_lua \
              --add-module=../ngx_dynamic_healthcheck > /dev/null 2>/dev/stderr

  r=$?
  if [ $r -ne 0 ]; then
    exit $r
  fi

  echo "Build release nginx-$VERSION$SUFFIX"
  make -j 8 > /dev/null 2>/dev/stderr

  r=$?
  if [ $r -ne 0 ]; then
    exit $r
  fi
  make install > /dev/null
  cd ..
}

function download_module() {
  if [ -e $DIR/../$2 ]; then
    echo "Get $DIR/../$2"
    dir=$(pwd)
    cd $DIR/..
    tar zcf $dir/$2.tar.gz $(ls -1d $2/* | grep -vE "(install$)|(build$)|(download$)|(.git$)")
    cd $dir
  else
    if [ $download -eq 1 ] || [ ! -e $2.tar.gz ]; then
      echo "Download $2 branch=$3"
      curl -s -L -O https://github.com/$1/$2/archive/$3.zip
      unzip -q $3.zip
      mv $2-$3 $2
      tar zcf $2.tar.gz $2
      rm -rf $2 $3.zip
    fi
  fi
}

function download_nginx() {
  if [ $download -eq 1 ] || [ ! -e nginx-$VERSION.tar.gz ]; then
    echo "Download nginx-$VERSION"
    curl -s -L -O http://nginx.org/download/nginx-$VERSION.tar.gz
  else
    echo "Get nginx-$VERSION.tar.gz"
  fi
}

function download_luajit() {
  if [ $download -eq 1 ] || [ ! -e LuaJIT-$LUAJIT_VERSION.tar.gz ]; then
    echo "Download LuaJIT-$LUAJIT_VERSION"
    curl -s -L -O http://luajit.org/download/LuaJIT-$LUAJIT_VERSION.tar.gz
  else
    echo "Get LuaJIT-$LUAJIT_VERSION.tar.gz"
  fi
}

function download_pcre() {
  if [ $download -eq 1 ] || [ ! -e pcre-$PCRE_VERSION.tar.gz ]; then
    echo "Download PCRE-$PCRE_VERSION"
    curl -s -L -O http://ftp.cs.stanford.edu/pub/exim/pcre/pcre-$PCRE_VERSION.tar.gz
  else
    echo "Get pcre-$PCRE_VERSION.tar.gz"
  fi
}

function extract_downloads() {
  cd download

  for d in $(ls -1 *.tar.gz)
  do
    echo "Extracting $d"
    tar zxf $d -C $BASE_PREFIX --keep-old-files 2>/dev/null
  done

  cd ..
}

function download() {
  mkdir build                2>/dev/null
  mkdir build/debug          2>/dev/null
  mkdir build/deps           2>/dev/null

  mkdir download             2>/dev/null
  mkdir download/debug       2>/dev/null
  mkdir download/lua_modules 2>/dev/null

  cd download

  download_nginx
  download_luajit
  download_pcre

  download_module ZigzagAK    ngx_dynamic_upstream             master
  download_module ZigzagAK    ngx_dynamic_upstream_lua         master
  download_module ZigzagAK    ngx_dynamic_healthcheck          master
  download_module simpl       ngx_devel_kit                    master
  download_module ZigzagAK    lua-nginx-module                 mixed
  download_module ZigzagAK    stream-lua-nginx-module          mixed
  download_module openresty   lua-cjson                        master

  cd ..
}

function install_file() {
  echo "Install $1"
  if [ ! -e "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/$2" ]; then
    mkdir -p "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/$2"
  fi
  cp -r $3 $1 "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/$2/"
}

function install_files() {
  for f in $(ls $1)
  do
    install_file $f $2 $3
  done
}

function build() {
  cd build

  patch -p0 < ../lua-cjson-Makefile.patch

  if [ $build_deps -eq 1 ] || [ ! -e deps/luajit ]; then
    build_luajit
  fi

  build_cJSON

  make clean > /dev/null 2>&1
  build_debug

  make clean > /dev/null 2>&1
  build_release

  install_file  "$JIT_PREFIX/usr/local/lib"           .
  install_file  lua-cjson/cjson.so                    lib/lua/5.1

  cd ..
}

clean
download
extract_downloads
build

function install_resty_module() {
  if [ -e $DIR/../$2 ]; then
    echo "Get $DIR/../$2"
    dir=$(pwd)
    cd $DIR/..
    zip -qr $dir/$2.zip $(ls -1d $2/* | grep -vE "(install$)|(build$)|(download$)|(.git$)")
    cd $dir
  else
    if [ $6 -eq 1 ] || [ ! -e $2-$5.zip ] ; then
      echo "Download $2 branch=$5"
      rm -rf $2-$5 2>/dev/null
      curl -s -L -O https://github.com/$1/$2/archive/$5.zip
      mv $5.zip $2-$5.zip
    else
      echo "Get $2-$5"
    fi
  fi
  echo "Install $2/$3"
  if [ ! -e "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/$4" ]; then
    mkdir -p "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/$4"
  fi
  if [ -e $2-$5.zip ]; then
    unzip -q $2-$5.zip
    cp -r $2-$5/$3 "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/$4/"
    rm -rf $2-$5
  elif [ -e $2-$5.tar.gz ]; then
    tar zxf $2-$5.tar.gz
    cp -r $2-$5/$3 "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/$4/"
    rm -rf $2-$5
  elif [ -e $2.zip ]; then
    unzip -q $2.zip
    cp -r $2/$3 "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/$4/"
    rm -rf $2
  elif [ -e $2.tar.gz ]; then
    tar zxf $2.tar.gz
    cp -r $2/$3 "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/$4/"
    rm -rf $2
  fi
}


function install_lua_modules() {
  if [ $download -eq 1 ]; then
    rm -rf download/lua_modules/* 2>/dev/null
  fi

  cd download/lua_modules

  install_resty_module openresty    lua-resty-lock                      lib                 .           master $download
  install_resty_module openresty    lua-resty-core                      lib                 .           master $download
  install_resty_module pintsized    lua-resty-http                      lib                 .           master $download
  install_resty_module ZigzagAK     amqp                                lib                 .           master $download

  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config scripts/start.sh                          .                     master $download
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config scripts/stop.sh                           .                     master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config scripts/debug.sh                          .                     master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config scripts/restart.sh                        .                     master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config lua/system.lua                            lua                   master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config lua/initd/init.lua                        lua/initd             master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config conf/conf.d/init                          conf/conf.d           master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config conf/conf.d/healthcheck                   conf/conf.d           master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config conf/conf.d/healthcheck.conf              conf/conf.d           master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config conf/conf.d/sysconfig/99-healthcheck.lua  conf/conf.d/sysconfig master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config conf/conf.d/sysconfig/healthcheck.ini     conf/conf.d/sysconfig master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config conf/conf.d/lastlog                       conf/conf.d           master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config conf/conf.d/lastlog.conf                  conf/conf.d           master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config lua/shdict.lua                            lua                   master 0
  install_resty_module ZigzagAK     nginx-resty-auto-healthcheck-config lua/shdict_ex.lua                         lua                   master 0


  cd ../..

  install_file lua  .
  install_file conf .
}

install_lua_modules

cp LICENSE "$INSTALL_PREFIX/nginx-$VERSION$SUFFIX/LICENSE"

cd "$DIR"

kernel_name=$(uname -s)
kernel_version=$(uname -r)

cd install

tar zcvf nginx-$VERSION$SUFFIX-$kernel_name-$kernel_version.tar.gz nginx-$VERSION$SUFFIX
rm -rf nginx-$VERSION$SUFFIX

cd ..

exit $r