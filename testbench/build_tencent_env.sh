# executable

yum -y update
yum install centos-release-scl -y
yum install devtoolset-9-gcc devtoolset-9-gcc-c++ -y
scl enable devtoolset-9 -- bash

yum install cmake3 -y

sudo alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake 10 \
--slave /usr/local/bin/ctest ctest /usr/bin/ctest \
--slave /usr/local/bin/cpack cpack /usr/bin/cpack \
--slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake \
--family cmake

sudo alternatives --install /usr/local/bin/cmake cmake /usr/bin/cmake3 20 \
--slave /usr/local/bin/ctest ctest /usr/bin/ctest3 \
--slave /usr/local/bin/cpack cpack /usr/bin/cpack3 \
--slave /usr/local/bin/ccmake ccmake /usr/bin/ccmake3 \
--family cmake


cd ..
chmod -R +x testbench/
chmod +x libs/build_libs.sh libs/clean_libs.sh