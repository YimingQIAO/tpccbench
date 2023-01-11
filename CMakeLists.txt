# cmake version
cmake_minimum_required(VERSION 3.0)

project(tpcc)

# c++ version
set(CMAKE_CXX_STANDARD 11)

SET(WARNINGS "-Werror -Wall -Wextra -Wno-sign-compare -Wno-unused-parameter")

# gcc flags used for both debug and opt builds
SET(CMAKE_CXX_FLAGS "-MD ${CMAKE_CXX_FLAGS} ${WARNINGS}")

# Debug flags
SET(CMAKE_CXX_FLAGS_DEBUG "-g ${CMAKE_CXX_FLAGS}")
SET(CMAKE_CXX_FLAGS_RELEASE "-g -O3 -DNDEBUG ${CMAKE_CXX_FLAGS}")

add_executable(btree_test btree_test.cc stupidunit.cc)
add_executable(randomgenerator_test randomgenerator_test.cc randomgenerator.cc stupidunit.cc)
add_executable(tpccclient_test tpccclient_test.cc tpccclient.cc randomgenerator.cc stupidunit.cc)
add_executable(tpcctables_test tpcctables_test.cc tpcctables.cc tpccdb.cc randomgenerator.cc stupidunit.cc)
add_executable(tpccgenerator tpccgenerator_test.cc tpccgenerator.cc tpcctables.cc tpccdb.cc randomgenerator.cc stupidunit.cc)
add_executable(tpcc tpcc.cc tpccclient.cc tpccgenerator.cc tpcctables.cc tpccdb.cc clock.cc randomgenerator.cc stupidunit.cc)

