#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
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
#include "strings/trim.h"

namespace fs = ::std::filesystem;

using ::f1_predict::load_csv;
using ::f1_predict::load_result;
using ::f1_predict::lookup_circuit;
using ::f1_predict::lookup_driver;
using ::f1_predict::lookup_team;
using ::f1_predict::parse_duration;
using ::f1_predict::parse_gap;
using ::f1_predict::parse_int;
using ::f1_predict::save_result;
using ::f1_predict::to_proto_duration;
using ::f1_predict::trim;
using ::std::chrono::hours;
using ::std::chrono::milliseconds;
using ::std::chrono::minutes;
using ::std::chrono::seconds;

ABSL_FLAG(std::string, input_file, "", "Path to CSV file containing data.");
ABSL_FLAG(
    std::string,
    output_dir,
    "",
    "Path to directory containing the imported data files.");
ABSL_FLAG(int, season, 0, "Year of the race season this data covers.");

const std::string CIRCUIT_COLUMN = "Track";
const std::string POSITION_COLUMN = "Position";
const std::string TIME_COLUMN = "Time/Retired";
const std::string TEAM_COLUMN = "Team";
const std::string DRIVER_COLUMN = "Driver";
const std::string STARTING_POSITION_COLUMN = "Starting Grid";
const std::string QUAL_1_COLUMN = "Q1";
const std::string QUAL_2_COLUMN = "Q2";
const std::string QUAL_3_COLUMN = "Q3";
constexpr std::string_view INPUT_EXTENSION = ".csv";
constexpr char DELIM = ',';
constexpr std::string_view DNF = "DNF";
constexpr std::string_view DNS = "DNS";
constexpr std::string_view DSQ = "DSQ";
constexpr std::string_view DQ = "DQ";
constexpr std::string_view NC = "NC";

void save_race_results(
    const std::vector<std::unordered_map<std::string, std::string>>& results,
    const fs::path& output_dir,
    int season,
    f1_predict::constants::Circuit circuit) {
  const std::unordered_map<std::string, std::string>* fastest_result = nullptr;
  for (const auto& result : results) {
    if (result.at(POSITION_COLUMN) == "1") {
      fastest_result = &result;
      break;
    }
  }
  if (fastest_result == nullptr) {
    std::cerr << "Failed to find first place position within results."
              << std::endl;
    std::exit(1);
  }

  milliseconds fastest_time = parse_duration(fastest_result->at(TIME_COLUMN));
  fs::create_directories(output_dir);
  for (const auto& result : results) {
    f1_predict::DriverResult proto_result;

    f1_predict::constants::Driver driver =
        lookup_driver(result.at(DRIVER_COLUMN));
    fs::path out_path = output_dir / f1_predict::constants::Driver_Name(driver);
    out_path += ".textproto";
    if (fs::exists(out_path)) proto_result = load_result(out_path);

    proto_result.set_circuit(circuit);
    proto_result.set_race_season(season);
    proto_result.set_team(lookup_team(result.at(TEAM_COLUMN)));
    proto_result.set_driver(driver);

    if (!result.at(STARTING_POSITION_COLUMN).empty()) {
      proto_result.set_starting_position(
          parse_int(result.at(STARTING_POSITION_COLUMN)));
    }
    if (!result.at(POSITION_COLUMN).empty() &&
        result.at(POSITION_COLUMN) != NC && result.at(POSITION_COLUMN) != DQ) {
      proto_result.set_final_position(parse_int(result.at(POSITION_COLUMN)));
    }
    if (proto_result.final_position() == 1) {
      *proto_result.mutable_finals_time() = to_proto_duration(fastest_time);
    } else if (
        result.at(TIME_COLUMN) != DNF && result.at(TIME_COLUMN) != DNS &&
        result.at(TIME_COLUMN) != DSQ) {
      *proto_result.mutable_finals_time() =
          to_proto_duration(fastest_time + parse_gap(result.at(TIME_COLUMN)));
    }

    save_result(out_path, proto_result);
  }
}

void save_qualification_results(
    const std::vector<std::unordered_map<std::string, std::string>>& results,
    const fs::path& output_dir,
    int season,
    f1_predict::constants::Circuit circuit) {

  fs::create_directories(output_dir);
  for (const auto& result : results) {
    f1_predict::DriverResult proto_result;

    f1_predict::constants::Driver driver =
        lookup_driver(result.at(DRIVER_COLUMN));
    fs::path out_path = output_dir / f1_predict::constants::Driver_Name(driver);
    out_path += ".textproto";
    if (fs::exists(out_path)) proto_result = load_result(out_path);

    proto_result.set_circuit(circuit);
    proto_result.set_race_season(season);
    proto_result.set_team(lookup_team(result.at(TEAM_COLUMN)));
    proto_result.set_driver(driver);

    if (result.at(POSITION_COLUMN) != NC && result.at(POSITION_COLUMN) != DQ) {
      *proto_result.mutable_qualification_time_1() =
          to_proto_duration(parse_duration(result.at(QUAL_1_COLUMN)));
      if (!result.at(QUAL_2_COLUMN).empty()) {
        *proto_result.mutable_qualification_time_2() =
            to_proto_duration(parse_duration(result.at(QUAL_2_COLUMN)));
      }
      if (!result.at(QUAL_3_COLUMN).empty()) {
        *proto_result.mutable_qualification_time_3() =
            to_proto_duration(parse_duration(result.at(QUAL_3_COLUMN)));
      }
    }

    save_result(out_path, proto_result);
  }
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  fs::path input_path = absl::GetFlag(FLAGS_input_file);
  if (input_path.extension() != INPUT_EXTENSION) {
    std::cerr << "Input file must be a CSV." << std::endl;
    return 1;
  }
  fs::path output_dir = absl::GetFlag(FLAGS_output_dir);
  if (output_dir.empty()) {
    std::cerr << "Must specify output directory." << std::endl;
    return 1;
  }
  if (absl::GetFlag(FLAGS_season) == 0) {
    std::cerr << "Must specify the race season." << std::endl;
    return 1;
  }

  std::ifstream input_stream{input_path};
  auto data = load_csv(input_stream);
  if (data.empty()) {
    std::cerr << "No data loaded." << std::endl;
    return 1;
  }

  std::unordered_map<
      f1_predict::constants::Circuit,
      std::vector<std::unordered_map<std::string, std::string>>>
      races_to_results;
  for (const auto& row : data) {
    races_to_results[lookup_circuit(row.at(CIRCUIT_COLUMN))].push_back(
        std::move(row));
  }

  std::string season_name = std::to_string(absl::GetFlag(FLAGS_season));
  if (output_dir.filename() != season_name) { output_dir /= season_name; }

  for (const auto& [circuit, results] : races_to_results) {
    if (results.front().contains(STARTING_POSITION_COLUMN)) {
      save_race_results(
          results,
          output_dir / f1_predict::constants::Circuit_Name(circuit),
          absl::GetFlag(FLAGS_season),
          circuit);
    } else if (results.front().contains(QUAL_1_COLUMN)) {
      save_qualification_results(
          results,
          output_dir / f1_predict::constants::Circuit_Name(circuit),
          absl::GetFlag(FLAGS_season),
          circuit);
    }
  }

  std::cout << "Processed " << data.size() << " results from "
            << races_to_results.size() << " circuits." << std::endl;
  return 0;
}
