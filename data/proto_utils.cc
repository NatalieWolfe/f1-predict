#include "data/proto_utils.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <system_error>

#include "data/race_results.pb.h"
#include "google/protobuf/duration.pb.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/util/time_util.h"

namespace f1_predict {

namespace fs = ::std::filesystem;

using ::google::protobuf::TextFormat;
using ::google::protobuf::util::TimeUtil;

f1_predict::DriverResult load_result(const fs::path& file_path) {
  std::ifstream stream(file_path);
  std::stringstream data;
  data << stream.rdbuf();
  f1_predict::DriverResult result;
  if (!TextFormat::ParseFromString(data.str(), &result)) {
    std::cerr << "Failed to parse result from " << file_path << std::endl;
    std::exit(1);
  }
  return result;
}

void save_result(const fs::path& file_path, const DriverResult& results) {
  std::string output;
  if (!TextFormat::PrintToString(results, &output)) {
    std::cerr << "Failed to print out race results.";
    std::exit(1);
  }
  std::ofstream out_stream{file_path};
  out_stream << output;
}

google::protobuf::Duration
to_proto_duration(std::chrono::milliseconds duration) {
  return TimeUtil::MillisecondsToDuration(duration.count());
}

std::chrono::milliseconds
to_milliseconds(const google::protobuf::Duration& duration) {
  return std::chrono::milliseconds{TimeUtil::DurationToMilliseconds(duration)};
}

} // namespace f1_predict
