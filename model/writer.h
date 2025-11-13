#pragma once

#include <filesystem>
#include <fstream>
#include <memory>
#include <span>
#include <vector>

#include "data/race_results.pb.h"
#include "model/data_aggregates.h"

namespace f1_predict {
namespace writer_internal {

class column_writer;

} // namespace writer_internal

struct writer_options {
  size_t race_size_limit = 20;
  char delim = ',';
};

class writer {
public:
  explicit writer(std::filesystem::path output_path, writer_options opts = {});

  void write_header();
  void write_race(
      std::span<const DriverResult> race_results,
      const historical_data& historical = {});

private:
  writer_options _options;
  std::filesystem::path _output_path;
  std::ofstream _out;
  std::vector<std::shared_ptr<writer_internal::column_writer>> _columns;
};

} // namespace f1_predict
