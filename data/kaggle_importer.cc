#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_join.h"
#include "data/constants.pb.h"
#include "data/constants_maps.h"
#include "data/csv.h"
#include "data/proto_utils.h"
#include "data/race_results.pb.h"
#include "strings/parse.h"
#include "strings/trim.h"

ABSL_FLAG(std::string, dir, "", "Root directory for kaggle dataset.");
ABSL_FLAG(
    std::string,
    output_dir,
    "",
    "Path to directory containing the imported data files.");

namespace fs = ::std::filesystem;

const fs::path CIRCUITS_FILE = "circuits.csv";
const fs::path CONSTRUCTORS_FILE = "constructors.csv";
const fs::path DRIVERS_FILE = "drivers.csv";
const fs::path QUALIFYING_FILE = "qualifying.csv";
const fs::path RACES_FILE = "races.csv";
const fs::path RESULTS_FILE = "results.csv";

constexpr std::string CIRCUIT_ID_COLUMN = "circuitId";
constexpr std::string CONSTRUCTOR_ID_COLUMN = "constructorId";
constexpr std::string DRIVER_ID_COLUMN = "driverId";
constexpr std::string QUALIFYING_ID_COLUMN = "qualifyId";
constexpr std::string RACE_ID_COLUMN = "raceId";
constexpr std::string RESULT_ID_COLUMN = "resultId";
constexpr std::string SEASON_COLUMN = "year";
constexpr std::string POSITION_COLUMN = "position";
constexpr std::string FINAL_POSITION_COLUMN = "positionOrder";
constexpr std::string STARTING_POSITION_COLUMN = "grid";
constexpr std::string FINAL_TIME_COLUMN = "time";
constexpr std::string QUAL_1_COLUMN = "q1";
constexpr std::string QUAL_2_COLUMN = "q2";
constexpr std::string QUAL_3_COLUMN = "q3";

constexpr std::string_view NULL_VALUE = "N";

using ::f1_predict::load_csv;
using ::f1_predict::load_result;
using ::f1_predict::lookup_circuit;
using ::f1_predict::lookup_driver;
using ::f1_predict::lookup_team;
using ::f1_predict::parse_duration;
using ::f1_predict::parse_int;
using ::f1_predict::save_result;
using ::f1_predict::to_proto_duration;
using ::f1_predict::trim;

struct IdMaps {
  std::unordered_map<int, f1_predict::constants::Circuit> circuit_map;
  std::unordered_map<int, f1_predict::constants::Team> team_map;
  std::unordered_map<int, f1_predict::constants::Driver> driver_map;
};

std::unordered_map<int, std::unordered_map<std::string, std::string>>
load_data(const fs::path& path, const std::string& id_column) {
  std::ifstream input_file{path};
  auto data = load_csv(input_file);

  std::unordered_map<int, std::unordered_map<std::string, std::string>> output;
  for (auto& row : data) {
    auto itr = row.find(id_column);
    if (itr == row.end()) {
      std::cerr << "Row missing id column \"" << id_column << "\"" << std::endl;
      std::exit(1);
    }
    output[parse_int(itr->second)] = std::move(row);
  }
  return output;
}

template <typename Enum, typename... NameColumns>
std::unordered_map<int, Enum> to_constants(
    const std::unordered_map<int, std::unordered_map<std::string, std::string>>&
        dataset,
    Enum (*lookup)(std::string_view),
    NameColumns&&... name_columns) {
  int failures_count = 0;
  std::unordered_map<int, Enum> mapper;
  for (const auto& [id, row] : dataset) {
    std::string name = absl::StrJoin({row.at(name_columns)...}, " ");
    std::string_view trimmed = trim(name);
    if (trimmed.empty()) {
      ((std::cerr << id << " missing columns ") << ... << name_columns)
          << std::endl;
      ++failures_count;
      continue;
    }
    mapper[id] = lookup(trimmed);
    if (!mapper[id]) ++failures_count;
  }
  if (failures_count) {
    std::cerr << "Encountered " << failures_count << " failures." << std::endl;
    exit(1);
  }
  return mapper;
}

fs::path results_file_path(
    const fs::path& output_dir,
    const std::unordered_map<std::string, std::string>& results,
    const IdMaps& id_maps,
    const std::unordered_map<std::string, std::string>& race) {
  fs::path file_path =
      output_dir / race.at(SEASON_COLUMN) /
      f1_predict::constants::Circuit_Name(
          id_maps.circuit_map.at(parse_int(race.at(CIRCUIT_ID_COLUMN)))) /
      f1_predict::constants::Driver_Name(
          id_maps.driver_map.at(parse_int(results.at(DRIVER_ID_COLUMN))));
  file_path += ".textproto";
  return file_path;
}

void save_finals_results(
    const std::unordered_map<std::string, std::string>& finals_result,
    const IdMaps& id_maps,
    const std::unordered_map<std::string, std::string>& race,
    const fs::path& results_file) {
  f1_predict::DriverResult result;
  if (fs::exists(results_file)) {
    result = load_result(results_file);
  } else {
    fs::create_directories(results_file.parent_path());
  }

  result.set_race_season(parse_int(race.at(SEASON_COLUMN)));
  result.set_circuit(
      id_maps.circuit_map.at(parse_int(race.at(CIRCUIT_ID_COLUMN))));
  result.set_driver(
      id_maps.driver_map.at(parse_int(finals_result.at(DRIVER_ID_COLUMN))));
  result.set_starting_position(
      parse_int(finals_result.at(STARTING_POSITION_COLUMN)));
  result.set_final_position(parse_int(finals_result.at(FINAL_POSITION_COLUMN)));

  if (!finals_result.at(FINAL_TIME_COLUMN).empty() &&
      finals_result.at(FINAL_TIME_COLUMN) != NULL_VALUE) {
    *result.mutable_qualification_time_1() =
        to_proto_duration(parse_duration(finals_result.at(FINAL_TIME_COLUMN)));
  }

  save_result(results_file, result);
}

void save_qualifying_results(
    const std::unordered_map<std::string, std::string>& qual_results,
    const IdMaps& id_maps,
    const std::unordered_map<std::string, std::string>& race,
    const fs::path& results_file) {
  f1_predict::DriverResult result;
  if (fs::exists(results_file)) {
    result = load_result(results_file);
  } else {
    fs::create_directories(results_file.parent_path());
  }

  result.set_race_season(parse_int(race.at(SEASON_COLUMN)));
  result.set_circuit(
      id_maps.circuit_map.at(parse_int(race.at(CIRCUIT_ID_COLUMN))));
  result.set_driver(
      id_maps.driver_map.at(parse_int(qual_results.at(DRIVER_ID_COLUMN))));
  result.set_starting_position(parse_int(qual_results.at(POSITION_COLUMN)));

  if (!qual_results.at(QUAL_1_COLUMN).empty() &&
      qual_results.at(QUAL_1_COLUMN) != NULL_VALUE) {
    *result.mutable_qualification_time_1() =
        to_proto_duration(parse_duration(qual_results.at(QUAL_1_COLUMN)));
  }
  if (!qual_results.at(QUAL_2_COLUMN).empty() &&
      qual_results.at(QUAL_2_COLUMN) != NULL_VALUE) {
    *result.mutable_qualification_time_2() =
        to_proto_duration(parse_duration(qual_results.at(QUAL_2_COLUMN)));
  }
  if (!qual_results.at(QUAL_3_COLUMN).empty() &&
      qual_results.at(QUAL_3_COLUMN) != NULL_VALUE) {
    *result.mutable_qualification_time_3() =
        to_proto_duration(parse_duration(qual_results.at(QUAL_3_COLUMN)));
  }

  save_result(results_file, result);
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  if (absl::GetFlag(FLAGS_dir).empty()) {
    std::cerr << "dir is required." << std::endl;
    return 1;
  }
  fs::path output_dir = absl::GetFlag(FLAGS_output_dir);
  if (output_dir.empty()) {
    std::cerr << "Must specify output directory." << std::endl;
    return 1;
  }

  const fs::path root = absl::GetFlag(FLAGS_dir);
  if (!fs::exists(root)) {
    std::cerr << "specified directory does not exist: " << root << std::endl;
    return 1;
  }
  fs::path circuits_file = root / CIRCUITS_FILE;
  fs::path constructors_file = root / CONSTRUCTORS_FILE;
  fs::path drivers_file = root / DRIVERS_FILE;
  fs::path qualifying_file = root / QUALIFYING_FILE;
  fs::path races_file = root / RACES_FILE;
  fs::path results_file = root / RESULTS_FILE;
  if (!fs::exists(circuits_file)) {
    std::cerr << "Missing circuits CSV file." << std::endl;
    return 1;
  }
  if (!fs::exists(constructors_file)) {
    std::cerr << "Missing constructors CSV file." << std::endl;
    return 1;
  }
  if (!fs::exists(drivers_file)) {
    std::cerr << "Missing drivers CSV file." << std::endl;
    return 1;
  }
  if (!fs::exists(qualifying_file)) {
    std::cerr << "Missing qualifying CSV file." << std::endl;
    return 1;
  }
  if (!fs::exists(races_file)) {
    std::cerr << "Missing races CSV file." << std::endl;
    return 1;
  }
  if (!fs::exists(results_file)) {
    std::cerr << "Missing results CSV file." << std::endl;
    return 1;
  }

  auto circuits = load_data(circuits_file, CIRCUIT_ID_COLUMN);
  auto constructors = load_data(constructors_file, CONSTRUCTOR_ID_COLUMN);
  auto drivers = load_data(drivers_file, DRIVER_ID_COLUMN);
  auto qualifying = load_data(qualifying_file, QUALIFYING_ID_COLUMN);
  auto races = load_data(races_file, RACE_ID_COLUMN);
  auto results = load_data(results_file, RESULT_ID_COLUMN);

  IdMaps id_maps{
      .circuit_map = to_constants(circuits, &lookup_circuit, "name"),
      .team_map = to_constants(constructors, &lookup_team, "name"),
      .driver_map =
          to_constants(drivers, &lookup_driver, "forename", "surname")};

  for (const auto& [qual_id, qual_results] : qualifying) {
    const auto& race = races.at(parse_int(qual_results.at(RACE_ID_COLUMN)));
    const fs::path results_file =
        results_file_path(output_dir, qual_results, id_maps, race);

    save_qualifying_results(qual_results, id_maps, race, results_file);
  }
  std::cout << "Imported " << qualifying.size() << " qualifying results"
            << std::endl;

  for (const auto& [result_id, finals_result] : results) {
    const auto& race = races.at(parse_int(finals_result.at(RACE_ID_COLUMN)));
    const fs::path results_file =
        results_file_path(output_dir, finals_result, id_maps, race);

    save_finals_results(finals_result, id_maps, race, results_file);
  }
  std::cout << "Imported " << results.size() << " finals results" << std::endl;

  return 0;
}
