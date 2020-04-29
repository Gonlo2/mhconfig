#include <csignal>
#include <stdlib.h>

#include <string>

#include "mhconfig/mhconfig.h"
#include "string_pool/pool.h"

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/async.h"

void signal_handler(int signum) {
  std::cout << "Interrupt signal (" << signum << ") received." << std::endl;

  exit(signum);
}

int run_mhconfig_server(int argc,char *argv[]) {
  //signal(SIGINT, signal_handler);

  auto console = spdlog::stdout_color_mt("console");

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

int run_sp(int argc,char *argv[]) {
  auto console = spdlog::stdout_color_mt("console");
  spdlog::set_level(spdlog::level::trace);
//
//  string_pool::Bucket<1<<16> bucket;
//
//  auto s1 = bucket.add("HolaMudo");
//  console->info(
//    "hash: {}, size: {}, refcount: {}, data: {}",
//    s1->hash,
//    s1->size,
//    s1->refcount,
//    std::string(&s1->data, s1->size)
//  );
//
//  auto s2 = bucket.add("Había");
//  console->info(
//    "hash: {}, size: {}, refcount: {}, data: {}",
//    s2->hash,
//    s2->size,
//    s2->refcount,
//    std::string(&s2->data, s2->size)
//  );
//
//  auto s3 = bucket.add("Había una vez");
//  console->info(
//    "hash: {}, size: {}, refcount: {}, data: {}",
//    s3->hash,
//    s3->size,
//    s3->refcount,
//    std::string(&s3->data, s3->size)
//  );
//
//  console->info(
//    "hash: {}, size: {}, refcount: {}, data: {}",
//    s1->hash,
//    s1->size,
//    s1->refcount,
//    std::string(&s1->data, s1->size)
//  );
//
//  std::unordered_set<string_pool::string_wrapper_t> string_set;
//  string_set.emplace(s1);
//  string_set.emplace(s2);
//
//  console->info("s1 = {}", string_set.count(s1));
//  console->info("s2 = {}", string_set.count(s2));
//  console->info("s3 = {}", string_set.count(s3));
//
//  std::cout << sizeof(string_pool::string_t) << std::endl;

  string_pool::Pool pool;
  auto s3 = pool.add("Hola");
  {
    auto s2 = pool.add("Mundo");
    auto s1 = pool.add("Hola");

    console->info(
      "hash: {}, data: {}",
      s1.hash(),
      s1.str()
    );

    console->info(
      "hash: {}, data: {}",
      s2.hash(),
      s2.str()
    );
  }
  auto s4 = pool.add("Mio");


  console->info(
    "hash: {}, data: {}",
    s3.hash(),
    s3.str()
  );


  console->info(
    "hash: {}, data: {}",
    s4.hash(),
    s4.str()
  );

  std::cout << sizeof(string_pool::string_t) << std::endl;
  std::cout << sizeof(string_pool::String) << std::endl;
  std::cout << sizeof(std::string) << std::endl;

  return 0;
}

int main(int argc,char *argv[]) {
  if (argc > 1) {
    if (strcmp(argv[1], "server") == 0) {
      return run_mhconfig_server(argc-2, &(argv[2]));
    } else if (strcmp(argv[1], "sp") == 0) {
      return run_sp(argc-2, &(argv[2]));
    }
  }

  return -1;
}
