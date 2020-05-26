cmake_minimum_required(VERSION 2.8.12)

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
endif()

set(CMAKE_C_COMPILER "gcc")
set(CMAKE_CXX_COMPILER "g++")

project(mhconfig)

enable_language(C CXX)

if(CMAKE_CXX_COMPILER_ID MATCHES GNU)
  set(CMAKE_CXX_FLAGS         "${CMAKE_CXX_FLAGS} -Wall")
  set(CMAKE_CXX_FLAGS_DEBUG   "-O0 -g3 -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE")
  set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DSPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO")
endif()

add_definitions(-std=c++17)

find_package(PkgConfig REQUIRED)
find_package(prometheus-cpp REQUIRED)
find_package(Catch2 REQUIRED)

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
add_executable(mhconfig src/mhconfig.cpp ${SRC_SOURCES})
target_link_libraries(mhconfig pthread)
target_link_libraries(mhconfig yaml-cpp)
target_link_libraries(mhconfig ssl.a crypto.a)
target_link_libraries(mhconfig boost_filesystem.a boost_system.a)
target_link_libraries(mhconfig prometheus-cpp::core prometheus-cpp::pull)
target_link_libraries(mhconfig cmph.a)
target_link_libraries(mhconfig debug libtcmalloc.so)
target_link_libraries(mhconfig optimized libtcmalloc_minimal.so)
target_link_libraries(mhconfig libstdc++.a) # probably it's possible remove this
target_link_libraries(mhconfig libgrpc++.a grpc.a)
target_include_directories(mhconfig PUBLIC ${GRPC_INCLUDE_DIRS})
target_compile_options(mhconfig PUBLIC ${GRPC_CFLAGS_OTHER})
target_link_libraries(mhconfig libprotobuf.a)

# tests
add_executable(tests tests/tests.cpp ${TESTS_SOURCES})
target_link_libraries(tests pthread)
target_link_libraries(tests yaml-cpp)
target_link_libraries(tests ssl.a crypto.a)
target_link_libraries(tests boost_filesystem.a boost_system.a)
target_link_libraries(tests prometheus-cpp::core prometheus-cpp::pull)
target_link_libraries(tests cmph.a)
target_link_libraries(tests libtcmalloc.so)
target_link_libraries(tests libgrpc++.a grpc.a)
target_include_directories(tests PUBLIC ${GRPC_INCLUDE_DIRS})
target_compile_options(tests PUBLIC ${GRPC_CFLAGS_OTHER})
target_link_libraries(tests libprotobuf.a)
