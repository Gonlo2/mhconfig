#include <stdlib.h>

#include <string>

#include "mhconfig/mhconfig.h"

#include "spdlog/spdlog.h"

int run_mhconfig_server(int argc,char *argv[]) {
  std::string server_address(argv[0]);
  std::string prometheus_address(argv[1]);
  size_t num_threads_api(std::atoi(argv[2]));
  size_t num_threads_workers(std::atoi(argv[3]));

  if (strcmp(argv[4], "trace") == 0) {
    spdlog::set_level(spdlog::level::trace);
  } else if (strcmp(argv[4], "debug") == 0) {
    spdlog::set_level(spdlog::level::debug);
  } else {
    spdlog::set_level(spdlog::level::info);
  }

  mhconfig::MHConfig server(
    server_address,
    prometheus_address,
    num_threads_api,
    num_threads_workers
  );

  server.run();
  server.join();

  return 0;
}

int main(int argc,char *argv[]) {
  if (argc > 1) {
    return run_mhconfig_server(argc-1, &(argv[1]));
  } else {
    std::cout << "Usage: " << argv[0] << " <gRPC listen address> <prometheus listen address> <num grpc threads> <num workers> <logger level>" << std::endl;
    std::cout << "example: " << argv[0] << " 0.0.0.0:2222 0.0.0.0:1111 13 13 info" << std::endl;
  }

  return -1;
}
