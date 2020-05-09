cmake_minimum_required(VERSION 2.8.12)

set(CMAKE_C_COMPILER "gcc")
set(CMAKE_CXX_COMPILER "g++")

# project name
project(mhconfig)

# enable fortran, c, and c++ language
enable_language(C CXX)

# example how to set c++ compiler flags for GNU
if(CMAKE_CXX_COMPILER_ID MATCHES GNU)
  #set(CMAKE_CXX_FLAGS         "${CMAKE_CXX_FLAGS} -Wall -fsanitize=address -fsanitize=leak")
  set(CMAKE_CXX_FLAGS         "${CMAKE_CXX_FLAGS} -Wall")
  set(CMAKE_CXX_FLAGS_DEBUG   "-O0 -g3")
  set(CMAKE_CXX_FLAGS_RELEASE "-O2")
endif()

add_definitions(-std=c++17)

set(Boost_USE_MULTITHREADED ON)

### PAQUETES #######################################

find_package(PkgConfig REQUIRED)
find_package(prometheus-cpp REQUIRED)


# location of header files
include_directories(
  ${PROJECT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}/src
)

file(
  GLOB_RECURSE
  SRC_SOURCES
  "src/**/*.cpp"
  "src/**/*.cc"
  "src/*.cpp"
)

# build executable
add_executable(mhconfig src/mhconfig.cpp ${SRC_SOURCES} ${mhconfig_service_sources})
target_link_libraries(mhconfig pthread)
target_link_libraries(mhconfig yaml-cpp)
target_link_libraries(mhconfig ssl.a crypto.a)
target_link_libraries(mhconfig boost_filesystem.a boost_system.a)
target_link_libraries(mhconfig prometheus-cpp::core prometheus-cpp::pull)
target_link_libraries(mhconfig cmph.a)
target_link_libraries(mhconfig libstdc++.a) # probably it's possible remove this

# gRPC
target_link_libraries(mhconfig libgrpc++.a grpc.a)
target_include_directories(mhconfig PUBLIC ${GRPC_INCLUDE_DIRS})
target_compile_options(mhconfig PUBLIC ${GRPC_CFLAGS_OTHER})
target_link_libraries(mhconfig libprotobuf.a)
