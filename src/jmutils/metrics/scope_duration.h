#ifndef JMUTILS__METRICS__SCOPE_DURATION_H
#define JMUTILS__METRICS__SCOPE_DURATION_H

#include <string>
#include <memory>
#include <prometheus/registry.h>

namespace jmutils
{
namespace metrics
{

class ScopeDuration
{
public:
  ScopeDuration(
    std::shared_ptr<prometheus::Registry> registry,
    const std::string& name,
    const std::string& help
  ):
    registry_(registry),
    name_(name),
    help_(help),
    start_time_(std::chrono::high_resolution_clock::now())
  {}

  virtual ~ScopeDuration () {
    //auto end_time = std::chrono::high_resolution_clock::now();

    //double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time_).count();

    //prometheus::Summary::Quantiles quantiles = {{0.5, 0.05}, {0.90, 0.01}, {0.99, 0.001}};

    //prometheus::BuildSummary()
      //.Name(name_)
      //.Help(help_)
      //.Register(*registry_)
      //.Add({}, quantiles)
      //.Observe(duration_ns);
  }

private:
  const std::shared_ptr<prometheus::Registry> registry_;
  const std::string name_;
  const std::string help_;
  const std::chrono::high_resolution_clock::time_point start_time_;
};


} /* metrics */
} /* jmutils */


#endif /* ifndef JMUTILS__METRICS__SCOPE_DURATION_H */
