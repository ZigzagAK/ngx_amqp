# Nginx AMQP

Nginx AMQP module

# Install

Build nginx amqp.
All dependencies are downloaded automaticaly.

Pre requirenments (for example centos/redhat)

```
sudo yum install gcc-c++.x86_64 zlib-devel openssl-devel
```

Build

```
git clone git@github.com:ZigzagAK/ngx_amqp.git
cd ngx_amqp
./build.sh
```

Archive will be placed in the `install` folder after successful build.