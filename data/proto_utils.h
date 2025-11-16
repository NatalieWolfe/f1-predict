#pragma once

#include <chrono>
#include <filesystem>
#include <span>
#include <vector>

#include "data/constants.pb.h"
#include "data/race_results.pb.h"
#include "google/protobuf/duration.pb.h"

namespace f1_predict {

DriverResult load_result(const std::filesystem::path& file_path);
DriverResult load_result(
    const std::filesystem::path& root_dir,
    int year,
    constants::Circuit circuit,
    constants::Driver driver);
std::vector<DriverResult>
load_all_results(std::span<const std::filesystem::path> paths);

void save_result(
    const std::filesystem::path& file_path, const DriverResult& results);

void save_prediction(
    const std::filesystem::path& file_path, const RacePrediction& results);

google::protobuf::Duration
to_proto_duration(std::chrono::milliseconds duration);

std::chrono::milliseconds
to_milliseconds(const google::protobuf::Duration& duration);

} // namespace f1_predict
