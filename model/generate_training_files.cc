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
#include "model/data_aggregates.h"
#include "model/writer.h"

ABSL_FLAG(
    std::string,
    training_file,
    "training.csv",
    "Path to save the training data.");

ABSL_FLAG(std::string, tests_file, "tests.csv", "Path to save test data.");
ABSL_FLAG(
    std::string, results_dir, "", "Path to directory containing race results.");

namespace fs = ::std::filesystem;

using ::f1_predict::constants::Circuit;
using ::f1_predict::constants::Driver;
using ::f1_predict::constants::Team;
using ::f1_predict::load_result;
using ::f1_predict::to_milliseconds;

using driver_to_results_map_t =
    ::std::unordered_map<Driver, ::f1_predict::DriverResult>;
using circuit_to_drivers_map_t =
    ::std::unordered_map<Circuit, ::driver_to_results_map_t>;
using season_to_circuit_map_t =
    ::std::unordered_map<int, ::circuit_to_drivers_map_t>;

std::vector<std::string> enumerate_files(const fs::path& root) {
  if (!fs::is_directory(root)) {
    if (!fs::exists(root)) return {};
    return {root};
  }

  std::vector<std::string> files;
  for (const fs::directory_entry& child : fs::directory_iterator(root)) {
    if (child.is_directory()) {
      std::ranges::move(
          enumerate_files(child.path()), std::back_inserter(files));
    }
    files.push_back(child.path());
  }
  return files;
}

std::vector<f1_predict::DriverResult>
load_all_data(std::span<std::string> file_paths) {
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
    organized[result.race_season()][result.circuit()][result.driver()] =
        std::move(result);
  }
  return organized;
}

bool is_valid_duration(const google::protobuf::Duration& duration) {
  return duration.seconds() || duration.nanos();
}

void filter_data(season_to_circuit_map_t& data) {
  season_to_circuit_map_t filtered;

  for (auto& [season, circuits] : data) {
    for (auto& [circuit, drivers] : circuits) {
      for (auto& [driver, results] : drivers) {
        if (is_valid_duration(results.qualification_time_1()) ||
            is_valid_duration(results.qualification_time_2()) ||
            is_valid_duration(results.qualification_time_3())) {
          filtered[season][circuit][driver] = std::move(results);
        }
      }
      if (filtered[season][circuit].size() < 5) {
        filtered[season].erase(circuit);
      }
    }
    if (filtered[season].empty()) filtered.erase(season);
  }

  data = std::move(filtered);
}

season_to_circuit_map_t extract_tests(season_to_circuit_map_t& data) {
  absl::BitGen bit_gen;
  season_to_circuit_map_t tests;
  for (auto& [season, circuits] : data) {
    if (circuits.size() == 1) continue;
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

template <std::ranges::range ResultList>
void add_race(f1_predict::historical_data& historical, const ResultList& race) {
  for (const f1_predict::DriverResult& result : race) {
    historical.circuit_drivers[result.circuit()][result.driver()]
        .finals_positions.push_back(result.final_position());
    historical.circuit_teams[result.circuit()][result.team()]
        .finals_positions.push_back(result.final_position());
    historical.driver_career[result.driver()].finals_positions.push_back(
        result.final_position());
  }
}

void save_data(
    const season_to_circuit_map_t& data, const fs::path& output_path) {
  f1_predict::writer out{output_path};
  f1_predict::historical_data historical;
  out.write_header();

  auto [first_season, last_season] =
      std::ranges::minmax_element(data | std::views::keys);

  for (int season = *first_season; season <= *last_season; ++season) {
    auto itr = data.find(season);
    if (itr == data.end()) continue;
    for (const auto& race : itr->second | std::views::values) {
      out.write_race(
          race | std::views::values | std::ranges::to<std::vector>(),
          historical);
      add_race(historical, race | std::views::values);
    }
  }
}

int main(int argc, char** argv) {
  std::vector<char*> args = absl::ParseCommandLine(argc, argv);
  auto input_files =
      args | std::views::drop(1) | std::ranges::to<std::vector<std::string>>();
  if (input_files.empty() && !absl::GetFlag(FLAGS_results_dir).empty()) {
    input_files = enumerate_files(absl::GetFlag(FLAGS_results_dir));
    std::cout << "Found " << input_files.size() << " files under "
              << absl::GetFlag(FLAGS_results_dir) << std::endl;
  }
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
  filter_data(data);
  season_to_circuit_map_t tests = extract_tests(data);

  save_data(data, training_file);
  save_data(tests, tests_file);

  return 0;
}
