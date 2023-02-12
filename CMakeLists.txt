# cmake version
cmake_minimum_required(VERSION 3.0)

project(tpcc)

# c++ version
set(CMAKE_CXX_STANDARD 17)

SET(WARNINGS "-Werror -Wall -Wextra -Wno-sign-compare -Wno-unused-parameter")

# gcc flags used for both debug and opt builds
SET(CMAKE_CXX_FLAGS "-MD ${CMAKE_CXX_FLAGS} ${WARNINGS}")

# Debug flags
SET(CMAKE_CXX_FLAGS_DEBUG "-g ${CMAKE_CXX_FLAGS}")
SET(CMAKE_CXX_FLAGS_RELEASE "-g -O3 -DNDEBUG ${CMAKE_CXX_FLAGS}")

add_executable(btree_test stupidunit.cc)
add_executable(randomgenerator_test randomgenerator.cc stupidunit.cc)
add_executable(tpccclient_test tpccclient.cc randomgenerator.cc stupidunit.cc)
add_executable(tpcctables_test tpcctables.cc tpccdb.cc randomgenerator.cc stupidunit.cc)
add_executable(tpccgenerator_test tpccgenerator.cc tpcctables.cc tpccdb.cc randomgenerator.cc stupidunit.cc)
add_executable(tpcc tpcc.cc tpccclient.cc tpccgenerator.cc tpcctables.cc tpccdb.cc clock.cc randomgenerator.cc stupidunit.cc disk_storage.h)
add_executable(disk_storage_test disk_storage.h disk_storage_test.cc)

