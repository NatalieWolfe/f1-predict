#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <ranges>
#include <span>
#include <string>
#include <system_error>
#include <unordered_map>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/random/random.h"
#include "data/constants.pb.h"
#include "data/proto_utils.h"
#include "data/race_results.pb.h"
#include "model/writer.h"

ABSL_FLAG(
    std::string,
    training_file,
    "training.csv",
    "Path to save the training data.");

ABSL_FLAG(std::string, tests_file, "tests.csv", "Path to save test data.");

namespace fs = ::std::filesystem;

using ::f1_predict::load_result;
using ::f1_predict::to_milliseconds;

using race_results = ::std::vector<::f1_predict::DriverResult>;
using circuit_to_drivers_map_t =
    ::std::unordered_map<::f1_predict::constants::Circuit, ::race_results>;
using season_to_circuit_map_t =
    ::std::unordered_map<int, ::circuit_to_drivers_map_t>;

constexpr int PLACEHOLDER_TIME = 9999999;

std::vector<f1_predict::DriverResult>
load_all_data(std::span<char*> file_paths) {
  std::vector<f1_predict::DriverResult> data;
  int error_count = 0;
  for (fs::path file_path : file_paths) {
    if (!fs::exists(file_path)) {
      std::cerr << "File not found: " << file_path << std::endl;
      ++error_count;
      continue;
    }
    data.push_back(load_result(file_path));
  }

  if (error_count > 0) {
    std::cerr << "Encountered " << error_count << " errors." << std::endl;
    std::exit(1);
  }
  return data;
}

season_to_circuit_map_t
organize_data(std::vector<f1_predict::DriverResult> raw_data) {
  season_to_circuit_map_t organized;
  for (f1_predict::DriverResult& result : raw_data) {
    organized[result.race_season()][result.circuit()].push_back(
        std::move(result));
  }
  return organized;
}

season_to_circuit_map_t extract_tests(season_to_circuit_map_t& data) {
  absl::BitGen bit_gen;
  season_to_circuit_map_t tests;
  for (auto& [season, circuits] : data) {
    std::size_t pick = absl::Uniform(bit_gen, 0u, circuits.size());
    std::size_t i = 0;
    for (auto& [circuit, results] : circuits) {
      if (++i == pick) {
        tests[season][circuit] = std::move(results);
        data[season].erase(circuit);
        break;
      }
    }
  }
  return tests;
}

int to_duration_training_value(const google::protobuf::Duration& duration) {
  if (duration.seconds() > 0 || duration.nanos() > 0) {
    return to_milliseconds(duration).count();
  }
  return PLACEHOLDER_TIME;
}

void save_data(
    const season_to_circuit_map_t& data, const fs::path& output_path) {
  f1_predict::writer out{output_path};
  out.write_header();

  for (const auto& circuits : data | std::views::values) {
    for (const auto& results : circuits | std::views::values) {
      out.write_race(results);
    }
  }
}

int main(int argc, char** argv) {
  std::vector<char*> args = absl::ParseCommandLine(argc, argv);
  std::span<char*> input_files{args.begin(), args.end()};
  input_files = input_files.subspan(1);
  if (input_files.empty()) {
    std::cerr << "Must specify at least 1 source file." << std::endl;
    return 1;
  }
  fs::path training_file = absl::GetFlag(FLAGS_training_file);
  if (training_file.empty()) {
    std::cerr << "Output training file must be specified." << std::endl;
    return 1;
  }
  fs::path tests_file = absl::GetFlag(FLAGS_tests_file);
  if (tests_file.empty()) {
    std::cerr << "Output training file must be specified." << std::endl;
    return 1;
  }
  if (training_file.has_parent_path()) {
    fs::create_directories(training_file.parent_path());
  }
  if (tests_file.has_parent_path()) {
    fs::create_directories(tests_file.parent_path());
  }

  auto raw_data = load_all_data(input_files);
  season_to_circuit_map_t data = organize_data(std::move(raw_data));
  season_to_circuit_map_t tests = extract_tests(data);

  save_data(data, training_file);
  save_data(tests, tests_file);

  return 0;
}
