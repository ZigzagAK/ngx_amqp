#!/bin/bash

# Copyright, Aleksey Konovkin (alkon2000@mail.ru)
# BSD license type

download=1
build_deps=1
build_debug=1
build_release=1

DIR="$(pwd)"

VERSION="1.11.6"

BASE_PREFIX="$DIR/build"
INSTALL_PREFIX="$DIR/install"

JIT_PREFIX="$DIR/build/deps/luajit"

export LUAJIT_INC="$JIT_PREFIX/usr/local/include/luajit-2.1"
export LUAJIT_LIB="$JIT_PREFIX/usr/local/lib"

export LD_LIBRARY_PATH="$JIT_PREFIX/lib"

function clean() {
  if [ $download -eq 1 ]; then
    rm -rf build    2>/dev/null
    rm -rf download 2>/dev/null
    rm -rf install  2>/dev/null
  else
    if [ $build_debug -eq 1 ] && [ $build_release -eq 1 ]; then
      if [ -e build ]; then
        cd build
        if [ $build_deps -eq 1 ]; then
          rm -rf deps/* 2>/dev/null
        fi
        cd nginx-$VERSION                 2>/dev/null && make clean > /dev/null 2>&1 ; cd $DIR/build
        cd LuaJIT-*                       2>/dev/null && make clean > /dev/null 2>&1 ; cd $DIR/build
        rm -rf nginx-$VERSION-*.tar.gz    2>/dev/null
        cd ..
      fi
    fi
  fi
}

if [ "$1" == "clean" ]; then
  clean
  exit 0
fi

function build_luajit() {
  echo "Build luajit"
  cd LuaJIT-*
  make -j 8 > /dev/null
  r=$?
  if [ $r -ne 0 ]; then
    exit $r
  fi
  DESTDIR="$JIT_PREFIX" make install > /dev/null
  cd ..
}

function build_debug() {
  cd nginx-${VERSION}
  if [ $build_debug -eq 1 ] && [ $build_release -eq 1 ]; then
    echo "Configuring debug nginx-${VERSION}"
    ./configure --prefix="$INSTALL_PREFIX/nginx-$VERSION-amqp" \
                --with-stream \
                --with-debug \
                --add-module=../ngx_devel_kit \
                --add-module=../lua-nginx-module \
                --add-module=../stream-lua-nginx-module > /dev/null
  fi

  r=$?
  if [ $r -ne 0 ]; then
    exit $r
  fi

  echo "Build debug nginx-${VERSION}-amqp"
  make -j 8 > /dev/null

  r=$?
  if [ $r -ne 0 ]; then
    exit $r
  fi
  make install > /dev/null

  mv "$INSTALL_PREFIX/nginx-$VERSION-amqp/sbin/nginx" "$INSTALL_PREFIX/nginx-$VERSION-amqp/sbin/nginx.debug"
  cd ..
}

function build_release() {
  cd nginx-${VERSION}
  echo "Configuring release nginx-${VERSION}"
  ./configure --prefix="$INSTALL_PREFIX/nginx-$VERSION-amqp" \
              --with-stream \
              --add-module=../ngx_devel_kit \
              --add-module=../lua-nginx-module \
              --add-module=../stream-lua-nginx-module > /dev/null

  r=$?
  if [ $r -ne 0 ]; then
    exit $r
  fi

  echo "Build release nginx-${VERSION}-amqp"
  make -j 8 > /dev/null

  r=$?
  if [ $r -ne 0 ]; then
    exit $r
  fi
  make install > /dev/null
  cd ..
}

function download_module() {
  echo "Download $2"
  rm -rf $2 $2-$3
  curl -s -L -O https://github.com/$1/$2/archive/$3.zip
  mv $3.zip $2.zip
  unzip -q $2.zip
  mv $2-$3 ../build/$2
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

function download_nginx() {
  echo "Download nginx-${VERSION}"
  curl -s -L -O http://nginx.org/download/nginx-${VERSION}.tar.gz
  tar zxf nginx-${VERSION}.tar.gz -C ../build
}

function download_luajit() {
  echo "Download LuaJIT"
  curl -s -L -O http://luajit.org/download/LuaJIT-2.1.0-beta2.tar.gz
  tar zxf LuaJIT-2.1.0-beta2.tar.gz -C ../build
}

function download() {
  if [ $download -eq 0 ]; then
    return
  fi

  mkdir build                2>/dev/null
  mkdir build/deps           2>/dev/null

  mkdir download             2>/dev/null
  mkdir download/debug       2>/dev/null
  mkdir download/lua_modules 2>/dev/null

  cd download

  download_nginx
  download_luajit

  download_module simpl       ngx_devel_kit                    master
  download_module openresty   lua-nginx-module                 master
  download_module ZigzagAK    stream-lua-nginx-module          fix-compile-1.11.4
  download_module openresty   lua-cjson                        master

  cd ..
}

function build() {
  cd build

  if [ $download -eq 1 ]; then
    patch -p0 < ../lua-cjson-Makefile.patch
  fi

  if [ $build_deps -eq 1 ]; then
    build_luajit
    build_cJSON
  fi

  if [ $build_debug -eq 1 ]; then
    make clean > /dev/null 2>&1
    build_debug
  fi

  if [ $build_release -eq 1 ]; then
    make clean > /dev/null 2>&1
    build_release
  fi

  cp -r "$JIT_PREFIX/usr/local/lib" "$INSTALL_PREFIX/nginx-$VERSION-amqp"
  pcrelib=$(ldd "$INSTALL_PREFIX/nginx-$VERSION-amqp/sbin/nginx" | grep pcre | awk '{print $3}')
  cp $pcrelib "$INSTALL_PREFIX/nginx-$VERSION-amqp/lib"
  cp lua-cjson/cjson.so "$INSTALL_PREFIX/nginx-$VERSION-amqp/lib/lua/5.1"
  cp ../scripts/* "$INSTALL_PREFIX/nginx-$VERSION-amqp"

  cd ..
}

clean
download
build

function install_resty_module() {
  if [ $download -eq 1 ]; then
    echo "Download $2"
    rm -rf $2-$4 2>/dev/null
    curl -s -L -O https://github.com/$1/$2/archive/$4.zip
    mv $4.zip $2-$4.zip
  fi
  echo "Install $2"
  unzip -q $2-$4.zip
  cp -r $2-$4/$3/* "$INSTALL_PREFIX/nginx-$VERSION-amqp/$3/"
  rm -rf $2-$4
}

function install_lua_modules() {
  if [ $download -eq 1 ]; then
    rm -rf download/lua_modules/* 2>/dev/null
  fi
  cd download/lua_modules

  install_resty_module openresty    lua-resty-lock                      lib master
  install_resty_module openresty    lua-resty-core                      lib master
  install_resty_module ZigzagAK     amqp                                lib master
  install_resty_module ZigzagAK     ngx_amqp                            .   master

  cd ../..
}

install_lua_modules

cp LICENSE "$INSTALL_PREFIX/nginx-$VERSION-amqp/LICENSE"

cd "$DIR"

kernel_name=$(uname -s)
kernel_version=$(uname -r)

cd install
tar zcvf nginx-$VERSION-amqp-$kernel_name-$kernel_version.tar.gz "nginx-$VERSION-amqp"
rm -rf nginx-$VERSION-amqp
cd ..