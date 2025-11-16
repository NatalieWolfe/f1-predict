#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <ranges>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "data/constants.pb.h"
#include "data/constants_maps.h"
#include "data/csv.h"
#include "data/proto_utils.h"
#include "data/race_results.pb.h"
#include "strings/parse.h"

ABSL_FLAG(std::string, source_results, "", "Path to the data being predicted.");
ABSL_FLAG(
    std::string,
    prediction_file,
    "",
    "Path to file containing the model's predictions.");
ABSL_FLAG(
    std::string,
    output,
    "",
    "Name of the output file to produce with the applied predictions.");

namespace fs = ::std::filesystem;

using ::f1_predict::constants::Circuit;
using ::f1_predict::constants::Driver;
using ::f1_predict::DriverPrediction;
using ::f1_predict::lookup_circuit_id;
using ::f1_predict::lookup_driver_id;
using ::f1_predict::parse_double;
using ::f1_predict::parse_int;

constexpr std::string SEASON_ID_COLUMN = "season_id";
constexpr std::string CIRCUIT_ID_COLUMN = "circuit_id";
constexpr std::string DRIVER_ID_COLUMN = "driver_id";

std::vector<std::unordered_map<std::string, std::string>>
load_source_data(const fs::path& path) {
  std::ifstream source_stream{path};
  return f1_predict::load_csv(source_stream);
}

std::vector<double> load_predictions(const fs::path& path, int expected_size) {
  std::ifstream prediction_stream{path};
  std::vector<double> predictions;
  predictions.reserve(expected_size);
  std::string line;
  while (std::getline(prediction_stream, line)) {
    predictions.push_back(parse_double(line));
  }
  return predictions;
}

double e_score(double prediction) { return std::exp(prediction); }

double p1_confidence(double prediction, double summed_e_score) {
  return e_score(prediction) / summed_e_score;
}

double direct_beat_confidence(double prediction, double prediction_to_beat) {
  return 1.0 / (1.0 + e_score(-(prediction - prediction_to_beat)));
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  fs::path source_results_file = absl::GetFlag(FLAGS_source_results);
  fs::path prediction_file = absl::GetFlag(FLAGS_prediction_file);
  fs::path output = absl::GetFlag(FLAGS_output);
  if (source_results_file.empty()) {
    std::cerr << "Source results file must be specified." << std::endl;
    return 1;
  }
  if (prediction_file.empty()) {
    std::cerr << "Prediction file must be specified." << std::endl;
    return 1;
  }
  if (output.empty()) {
    std::cerr << "Output file must be specified." << std::endl;
    return 1;
  }

  std::cout << "Loading source: " << source_results_file << " : "
            << fs::file_size(source_results_file) << std::endl;
  auto source = load_source_data(source_results_file);
  std::cout << "Loading predictions: " << prediction_file << std::endl;
  auto predictions = load_predictions(prediction_file, source.size());
  if (source.size() != predictions.size()) {
    std::cerr << "Mismatched predictions to source data." << std::endl;
    std::exit(1);
  }

  std::unordered_map<
      int,
      std::unordered_map<Circuit, std::unordered_map<Driver, DriverPrediction>>>
      races;
  double summed_e_score = 0;
  for (size_t i = 0; i < predictions.size(); ++i) {
    int year = parse_int(source[i][SEASON_ID_COLUMN]) + 1900;
    Circuit circuit = lookup_circuit_id(source[i][CIRCUIT_ID_COLUMN]);
    Driver driver = lookup_driver_id(source[i][DRIVER_ID_COLUMN]);

    races[year][circuit][driver].set_driver(driver);
    races[year][circuit][driver].set_raw_prediction(predictions[i]);

    summed_e_score += e_score(predictions[i]);

    f1_predict::DriverResult result =
        f1_predict::load_result("data/result", year, circuit, driver);
    if (result.final_position()) {
      races[year][circuit][driver].set_real_position(result.final_position());
    }
  }

  if (races.empty()) {
    std::cerr << "No results found." << std::endl;
    return 1;
  }
  if (races.size() > 1 || races.begin()->second.size() != 1) {
    std::cerr << "Multiple races found!" << std::endl;
    return 1;
  }

  auto prediction_data = races | std::views::values | std::views::join |
      std::views::values | std::views::join | std::views::values |
      std::ranges::to<std::vector<DriverPrediction>>();
  std::ranges::sort(prediction_data, [&](const auto& a, const auto& b) {
    if (a.raw_prediction() != b.raw_prediction()) {
      return a.raw_prediction() > b.raw_prediction();
    }
    return a.driver() < b.driver();
  });
  double p4_score = prediction_data[3].raw_prediction();
  for (size_t i = 0; i < prediction_data.size(); ++i) {
    prediction_data[i].set_position(i + 1);
    prediction_data[i].set_p1_confidence(
        p1_confidence(prediction_data[i].raw_prediction(), summed_e_score));
    prediction_data[i].set_top_3_confidence(
        direct_beat_confidence(prediction_data[i].raw_prediction(), p4_score));
  }

  f1_predict::RacePrediction race_prediction;
  race_prediction.set_race_season(races.begin()->first);
  race_prediction.set_circuit(races.begin()->second.begin()->first);
  race_prediction.mutable_predictions()->Assign(
      prediction_data.begin(), prediction_data.end());

  if (output.has_parent_path() && !fs::exists(output.parent_path())) {
    fs::create_directories(output.parent_path());
  }
  f1_predict::save_prediction(output, race_prediction);

  return 0;
}
