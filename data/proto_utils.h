#pragma once

#include <chrono>
#include <filesystem>

#include "data/race_results.pb.h"
#include "google/protobuf/duration.pb.h"

namespace f1_predict {

DriverResult load_result(const std::filesystem::path& file_path);
void save_result(
    const std::filesystem::path& file_path, const DriverResult& results);

google::protobuf::Duration
to_proto_duration(std::chrono::milliseconds duration);

std::chrono::milliseconds
to_milliseconds(const google::protobuf::Duration& duration);

} // namespace f1_predict
