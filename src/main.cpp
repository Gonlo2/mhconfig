#include <cstdlib>
#include <iostream>
#include <string>

#include "mhconfig/mhconfig.h"
#include "mhconfig/local.h"
#include "spdlog/cfg/env.h"

void prepare_logger() {
  spdlog::cfg::load_env_levels();

  auto logger = spdlog::create<spdlog::sinks::stderr_color_sink_mt>("console");
  spdlog::set_default_logger(logger);
}

int run_mhconfig_server(int argc, char *argv[]) {
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

int run_mhconfig_local(int argc, char *argv[]) {
  mhconfig::is_worker_thread(true);

  mhconfig::context_t ctx;

  while (true) {
    if (int res = mhconfig::print_stdin_config(&ctx)) {
      return res-1;
    }
  }

  return 0;
}

int main(int argc,char *argv[]) {
  if (argc <= 1) {
    std::cout << "Usage: " << argv[0] << " [daemon|local] ..." << std::endl;
    std::cout << std::endl;
    std::cout << "Server mode: " << argv[0] << " daemon <daemon config path> <gRPC listen address> <prometheus listen address> <num grpc threads> <num workers>" << std::endl;
    std::cout << std::endl;
    std::cout << "Local mode: " << argv[0] << " local" << std::endl;
    std::cout << "    The parameters are read from the standard input in blocks divided" << std::endl;
    std::cout << "    by an empty line and the output is a YAML document per input block." << std::endl;
    std::cout << "    The first line is the path of the config folder, the second line" << std::endl;
    std::cout << "    is the document name and the rest of lines are the labels with" << std::endl;
    std::cout << "    the format <key>/<value>." << std::endl;
    return 1;
  }

  prepare_logger();

  if (strcmp(argv[1], "server") == 0) {
    return run_mhconfig_server(argc-2, &(argv[2]));
  } else if (strcmp(argv[1], "local") == 0) {
    return run_mhconfig_local(argc-2, &(argv[2]));
  } else {
    std::cout << "Unknown mode '" << argv[1] << "'" << std::endl;
  }

  return -1;
}
