cmake_minimum_required(VERSION 2.8.12)

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
endif()

set(CMAKE_C_COMPILER "gcc")
set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_CXX_STANDARD 17)

project(mhconfig)

enable_language(C CXX)

if(CMAKE_CXX_COMPILER_ID MATCHES GNU)
  set(CMAKE_CXX_FLAGS         "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unused-parameter")
  set(CMAKE_CXX_FLAGS_DEBUG   "-O0 -g3 -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE")
  set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO")
endif()

find_package(prometheus-cpp REQUIRED)
find_package(Catch2 REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

add_subdirectory(third_party/abseil-cpp)

# location of header files
include_directories(${PROJECT_SOURCE_DIR}/src)

file(
  GLOB_RECURSE
  SRC_SOURCES
  "src/**/*.cpp"
  "src/**/*.cc"
  "src/*.cpp"
)

file(
  GLOB_RECURSE
  TESTS_SOURCES
  "src/**/*.cpp"
  "src/**/*.cc"
  "tests/**/*.cpp"
  "tests/**/*.cc"
  "tests/*.cpp"
)

# build executable
add_executable(mhconfig src/main.cpp ${SRC_SOURCES})
target_link_libraries(mhconfig pthread)
target_link_libraries(mhconfig ssl.a crypto.a)
target_link_libraries(mhconfig yaml-cpp)
target_link_libraries(mhconfig prometheus-cpp::core prometheus-cpp::pull)
target_link_libraries(mhconfig optimized libtcmalloc_minimal.so)
target_link_libraries(mhconfig stdc++.a stdc++fs.a)
target_link_libraries(mhconfig gRPC::grpc++)
target_link_libraries(mhconfig absl::synchronization absl::flat_hash_map)


# tests
add_executable(tests tests/tests.cpp ${TESTS_SOURCES})
target_link_libraries(tests pthread)
target_link_libraries(tests ssl crypto)
target_link_libraries(tests stdc++fs)
target_link_libraries(tests yaml-cpp)
target_link_libraries(tests prometheus-cpp::core prometheus-cpp::pull)
target_link_libraries(mhconfig gRPC::grpc++)
