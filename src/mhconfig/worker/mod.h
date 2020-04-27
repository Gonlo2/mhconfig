#ifndef MHCONFIG__WORKER__MOD_H
#define MHCONFIG__WORKER__MOD_H
//
//#include <thread>
//#include <random>
//
//#include "jmutils/container/queue.h"
//#include "jmutils/parallelism/worker.h"
//#include "jmutils/filesystem/common.h"
//
////#include "mhconfig/worker/common.h"
//#include "mhconfig/api/request/request.h"
//#include "mhconfig/api/request/get_request.h"
//#include "mhconfig/api/request/update_request.h"
//
//namespace mhconfig
//{
//namespace worker
//{
//
//using jmutils::container::Queue;
//using namespace mhconfig::api::request;
//
//class Worker : public jmutils::parallelism::Worker<command::CommandRef>
//{
//public:
//  Worker(
//    Queue<command::CommandRef>& worker_queue,
//    size_t num_threads,
//    Queue<mhconfig::scheduler::command::CommandRef>& scheduler_queue,
//    Metrics& metrics
//  );
//
//  virtual ~Worker();
//
//  Worker(Worker&& o);
//
//protected:
//  const std::string worker_name() const override {
//    return "builder";
//  }
//
//  ProcessResult worker_process(command::CommandRef command) override;
//
//private:
//  Metrics& metrics_;
//  Queue<command::command_t>& scheduler_queue_;
//};
//
//} /* worker */
//} /* mhconfig */
//

#endif
