#ifndef MHCONFIG__LOCAL_H
#define MHCONFIG__LOCAL_H

#include "jmutils/container/label_set.h"
#include "mhconfig/element.h"
#include "mhconfig/config_namespace.h"
#include "mhconfig/provider.h"
#include "mhconfig/logger/spdlog_logger.h"

namespace mhconfig
{

using jmutils::container::Labels;
using jmutils::container::label_t;
using jmutils::container::make_labels;

class ConsoleGetConfigTask final
  : public mhconfig::GetConfigTask
{
public:
  ConsoleGetConfigTask(
    std::string&& root_path,
    Labels&& labels,
    std::string&& document
  ) : root_path_(std::move(root_path)),
    labels_(std::move(labels)),
    document_(std::move(document))
  {
  };
  ~ConsoleGetConfigTask() {
  }

  const std::string& root_path() const override {
    return root_path_;
  }

  uint32_t version() const override {
    return 0;
  }

  const Labels& labels() const override {
    return labels_;
  }

  const std::string& document() const override {
    return document_;
  }

  void on_complete(
    std::shared_ptr<mhconfig::config_namespace_t>& cn,
    mhconfig::VersionId version,
    const mhconfig::Element& element,
    const std::array<uint8_t, 32>& checksum,
    void* payload
  ) override {
    if (auto y = element.to_yaml()) {
      std::cout << *y;
    } else {
      spdlog::error("Some error take place making the yaml");
      std::cout << "---" << std::endl;
      std::cout << "..." << std::endl;
    }
  }

  Logger& logger() override {
    return logger_;
  }

  ReplayLogger::Level log_level() const override {
    return ReplayLogger::Level::warn;
  }

private:
  logger::SpdlogLogger logger_;
  std::string root_path_;
  Labels labels_;
  std::string document_;
};

int print_stdin_config(
  context_t* ctx
) {
  std::string root_path;
  if (!std::getline(std::cin, root_path)) return 1;

  std::string document;
  if (!std::getline(std::cin, document)) return 2;

  std::vector<label_t> tmp_labels;
  for (std::string line; std::getline(std::cin, line);) {
    if (line.empty()) break;
    auto pos = line.find('/');
    if (pos == std::string::npos) {
      spdlog::error(
        "The label '{}' must have a '/' simbol to separate the key of the value",
        line
      );
    }
    auto key = line.substr(0, pos);
    auto value = line.substr(pos+1, line.size()-pos-1);
    tmp_labels.emplace_back(std::move(key), std::move(value));
  }

  auto labels = make_labels(std::move(tmp_labels));

  bool ok = mhconfig::validator::are_valid_arguments(
    root_path,
    labels,
    document
  );

  if (!ok) {
    spdlog::error("The provided arguments are incorrect");
    return 2;
  }

  auto cn = get_or_build_cn(ctx, root_path);
  mhconfig::process_get_config_task(
    std::move(cn),
    std::make_shared<ConsoleGetConfigTask>(
      std::move(root_path),
      std::move(labels),
      std::move(document)
    ),
    ctx
  );

  return 0;
}

} /* mhconfig */

#endif
