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
namespace {

namespace fs = ::std::filesystem;

using ::google::protobuf::TextFormat;
using ::google::protobuf::util::TimeUtil;

bool zero_duration(const google::protobuf::Duration& duration) {
  return duration.seconds() == 0 && duration.nanos() == 0;
}

} // namespace

f1_predict::DriverResult load_result(const fs::path& file_path) {
  std::ifstream stream(file_path);
  std::stringstream data;
  data << stream.rdbuf();
  f1_predict::DriverResult result;
  if (!TextFormat::ParseFromString(data.str(), &result)) {
    std::cerr << "Failed to parse result from " << file_path << std::endl;
    std::exit(1);
  }

  if (result.has_qualification_time_1() &&
      zero_duration(result.qualification_time_1())) {
    result.clear_qualification_time_1();
  }
  if (result.has_qualification_time_2() &&
      zero_duration(result.qualification_time_2())) {
    result.clear_qualification_time_2();
  }
  if (result.has_qualification_time_3() &&
      zero_duration(result.qualification_time_3())) {
    result.clear_qualification_time_3();
  }
  if (result.has_finals_time() && zero_duration(result.finals_time())) {
    result.clear_finals_time();
  }
  if (result.has_finals_fastest_lap_time() &&
      zero_duration(result.finals_fastest_lap_time())) {
    result.clear_finals_fastest_lap_time();
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
