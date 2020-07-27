#include <stdlib.h>

#include <string>

#include "mhconfig/mhconfig.h"

#include "spdlog/spdlog.h"
#include "spdlog/cfg/env.h"

int run_mhconfig_server(int argc,char *argv[]) {
  spdlog::cfg::load_env_levels();

  std::string mhconfig_config_path(argv[0]);
  std::string server_address(argv[1]);
  std::string prometheus_address(argv[2]);
  size_t num_threads_api(std::atoi(argv[3]));
  size_t num_threads_workers(std::atoi(argv[4]));

  mhconfig::MHConfig server(
    mhconfig_config_path,
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
    std::cout << "Usage: " << argv[0] << " <daemon config path> <gRPC listen address> <prometheus listen address> <num grpc threads> <num workers>" << std::endl;
    std::cout << "example: " << argv[0] << " daemon_config 0.0.0.0:2222 0.0.0.0:1111 13 13" << std::endl;
  }

  return -1;
}
