#include <cctype>
#include <charconv>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "data/constants.pb.h"
#include "data/race_results.pb.h"
#include "google/protobuf/duration.pb.h"
#include "google/protobuf/text_format.h"
#include "google/protobuf/util/time_util.h"

namespace fs = ::std::filesystem;

using ::google::protobuf::TextFormat;
using ::google::protobuf::util::TimeUtil;
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
constexpr std::errc NO_ERROR{};
constexpr std::string_view DNF = "DNF";
constexpr std::string_view DNS = "DNS";
constexpr std::string_view DSQ = "DSQ";
constexpr std::string_view DQ = "DQ";
constexpr std::string_view NC = "NC";

const std::unordered_map<std::string_view, f1_predict::constants::Circuit>
    NAME_TO_CIRCUIT_MAP = {
        {"Australia", f1_predict::constants::AUSTRALIA_CIRCUIT},
        {"China", f1_predict::constants::CHINA_CIRCUIT},
        {"Japan", f1_predict::constants::JAPAN_CIRCUIT},
        {"Bahrain", f1_predict::constants::BAHRAIN_CIRCUIT},
        {"Saudi Arabia", f1_predict::constants::SAUDI_ARABIA_CIRCUIT},
        {"Miami", f1_predict::constants::MIAMI_CIRCUIT},
        {"Emilia-Romagna", f1_predict::constants::EMILIA_ROMAGNA_CIRCUIT},
        {"Emilia Romagna", f1_predict::constants::EMILIA_ROMAGNA_CIRCUIT},
        {"Monaco", f1_predict::constants::MONACO_CIRCUIT},
        {"Spain", f1_predict::constants::SPAIN_CIRCUIT},
        {"Canada", f1_predict::constants::CANADA_CIRCUIT},
        {"Austria", f1_predict::constants::AUSTRIA_CIRCUIT},
        {"Great Britain", f1_predict::constants::GREAT_BRITAIN_CIRCUIT},
        {"Belgium", f1_predict::constants::BELGIUM_CIRCUIT},
        {"Hungary", f1_predict::constants::HUNGARY_CIRCUIT},
        {"Netherlands", f1_predict::constants::NETHERLANDS_CIRCUIT},
        {"Italy", f1_predict::constants::ITALY_CIRCUIT},
        {"Azerbaijan", f1_predict::constants::AZERBAIJAN_CIRCUIT},
        {"Singapore", f1_predict::constants::SINGAPORE_CIRCUIT},
        {"United States", f1_predict::constants::UNITED_STATES_CIRCUIT},
        {"Mexico", f1_predict::constants::MEXICO_CIRCUIT},
        {"Brazil", f1_predict::constants::BRAZIL_CIRCUIT},
        {"Las Vegas", f1_predict::constants::LAS_VEGAS_CIRCUIT},
        {"Qatar", f1_predict::constants::QATAR_CIRCUIT},
        {"Abu Dhabi", f1_predict::constants::ABU_DHABI_CIRCUIT},
        {"France", f1_predict::constants::FRANCE_CIRCUIT},
        {"Portugal", f1_predict::constants::PORTUGAL_CIRCUIT},
        {"Styria", f1_predict::constants::STYRIA_CIRCUIT},
        {"Russia", f1_predict::constants::RUSSIA_CIRCUIT},
        {"Turkiye", f1_predict::constants::TURKIYE_CIRCUIT},
        {"Turkey", f1_predict::constants::TURKIYE_CIRCUIT}};

const std::unordered_map<std::string_view, f1_predict::constants::Team>
    NAME_TO_TEAM_MAP = {
        {"McLaren", f1_predict::constants::MCLAREN},
        {"McLaren Mercedes", f1_predict::constants::MCLAREN},
        {"Ferrari", f1_predict::constants::FERRARI},
        {"Red Bull Racing", f1_predict::constants::RED_BULL_RACING},
        {"Red Bull Racing Honda RBPT", f1_predict::constants::RED_BULL_RACING},
        {"Red Bull Racing honda RBPT", f1_predict::constants::RED_BULL_RACING},
        {"Red Bull Racing Honda EBPT", f1_predict::constants::RED_BULL_RACING},
        {"Red Bull Racing RBPT", f1_predict::constants::RED_BULL_RACING},
        {"Red Bull Racing Honda", f1_predict::constants::RED_BULL_RACING},
        {"Mercedes", f1_predict::constants::MERCEDES},
        {"Aston Martin", f1_predict::constants::ASTON_MARTIN},
        {"Aston Martin Aramco Mercedes", f1_predict::constants::ASTON_MARTIN},
        {"Aston Martin Mercedes", f1_predict::constants::ASTON_MARTIN},
        {"Alpine", f1_predict::constants::ALPINE},
        {"Alpine Renault", f1_predict::constants::ALPINE},
        {"Haas", f1_predict::constants::HAAS},
        {"Haas Ferrari", f1_predict::constants::HAAS},
        {"RB", f1_predict::constants::RB},
        {"Racing Bulls Honda RBPT", f1_predict::constants::RB},
        {"Racing bulls Honda RBPT", f1_predict::constants::RB},
        {"Racing Honda RBPT", f1_predict::constants::RB},
        {"RB Honda RBPT", f1_predict::constants::RB},
        {"AlphaTauri", f1_predict::constants::RB},
        {"AlphaTauri RBPT", f1_predict::constants::RB},
        {"AlphaTauri Honda RBPT", f1_predict::constants::RB},
        {"AlphaTauri Honda", f1_predict::constants::RB},
        {"Williams", f1_predict::constants::WILLIAMS},
        {"Williams Mercedes", f1_predict::constants::WILLIAMS},
        {"Kick Sauber", f1_predict::constants::KICK_SAUBER},
        {"Kick Sauber Ferrari", f1_predict::constants::KICK_SAUBER},
        {"Alfa Romeo Ferrari", f1_predict::constants::KICK_SAUBER},
        {"Alfa Romeo Racing Ferrari", f1_predict::constants::KICK_SAUBER}};

const std::unordered_map<std::string_view, f1_predict::constants::Driver>
    NAME_TO_DRIVER_MAP = {
        {"Alexander Albon", f1_predict::constants::ALEXANDER_ALBON},
        {"Carlos Sainz", f1_predict::constants::CARLOS_SAINZ},
        {"Charles Leclerc", f1_predict::constants::CHARLES_LECLERC},
        {"Esteban Ocon", f1_predict::constants::ESTEBAN_OCON},
        {"Fernando Alonso", f1_predict::constants::FERNANDO_ALONSO},
        {"Gabriel Bortoleto", f1_predict::constants::GABRIEL_BORTOLETO},
        {"George Russell", f1_predict::constants::GEORGE_RUSSELL},
        {"Isack Hadjar", f1_predict::constants::ISACK_HADJAR},
        {"Jack Doohan", f1_predict::constants::JACK_DOOHAN},
        {"Kimi Antonelli", f1_predict::constants::KIMI_ANTONELLI},
        {"Lance Stroll", f1_predict::constants::LANCE_STROLL},
        {"Lando Norris", f1_predict::constants::LANDO_NORRIS},
        {"Lewis Hamilton", f1_predict::constants::LEWIS_HAMILTON},
        {"Liam Lawson", f1_predict::constants::LIAM_LAWSON},
        {"Max Verstappen", f1_predict::constants::MAX_VERSTAPPEN},
        {"Nico Hulkenberg", f1_predict::constants::NICO_HULKENBERG},
        {"Oliver Bearman", f1_predict::constants::OLIVER_BEARMAN},
        {"Oscar Piastri", f1_predict::constants::OSCAR_PIASTRI},
        {"Pierre Gasly", f1_predict::constants::PIERRE_GASLY},
        {"Yuki Tsunoda", f1_predict::constants::YUKI_TSUNODA},
        {"Franco Colapinto", f1_predict::constants::FRANCO_COLAPINTO},
        {"Valtteri Bottas", f1_predict::constants::VALTTERI_BOTTAS},
        {"Sergio Perez", f1_predict::constants::SERGIO_PEREZ},
        {"Kevin Magnussen", f1_predict::constants::KEVIN_MAGNUSSEN},
        {"Guanyu Zhou", f1_predict::constants::GUANYU_ZHOU},
        {"Daniel Ricciardo", f1_predict::constants::DANIEL_RICCIARDO},
        {"Logan Sargeant", f1_predict::constants::LOGAN_SARGEANT},
        {"Nyck De Vries", f1_predict::constants::NYCK_DE_VRIES},
        {"Sebastian Vettel", f1_predict::constants::SEBASTIAN_VETTEL},
        {"Mick Schumacher", f1_predict::constants::MICK_SCHUMACHER},
        {"Nicholas Latifi", f1_predict::constants::NICHOLAS_LATIFI},
        {"Antonio Giovinazzi", f1_predict::constants::ANTONIO_GIOVINAZZI},
        {"Kimi Raikkönen", f1_predict::constants::KIMI_RAIKKONEN},
        {"Nikita Mazepin", f1_predict::constants::NIKITA_MAZEPIN},
        {"Robert Kubica", f1_predict::constants::ROBERT_KUBICA}};

std::string trim(std::string term) {
  uint64_t start_pos = 0;
  for (; start_pos < term.size() && std::isspace(term[start_pos]);
       ++start_pos) {}
  int64_t end_pos = term.size() - 1;
  for (; end_pos >= 0 && std::isspace(term[end_pos]); --end_pos) {}

  int64_t len = end_pos - start_pos + 1;
  if (start_pos > 0) {
    for (int64_t i = 0; i < len; ++i) {
      term[i] = term[i] + start_pos;
    }
  }
  term.resize(len);
  return std::move(term);
}

std::vector<std::unordered_map<std::string, std::string>>
load_input(const fs::path& input_path) {
  std::ifstream file(input_path);

  std::string header_line;
  if (!std::getline(file, header_line)) {
    std::cerr << "Input file is empty." << std::endl;
    std::exit(1);
  }

  std::stringstream header_stream(header_line);
  std::vector<std::string> column_names;
  for (std::string column; std::getline(header_stream, column, DELIM);) {
    column_names.push_back(trim(std::move(column)));
  }

  std::vector<std::unordered_map<std::string, std::string>> content;
  for (std::string line; std::getline(file, line);) {
    std::stringstream line_stream(line);
    auto& row = content.emplace_back();
    std::string column;
    for (int i = 0; std::getline(line_stream, column, DELIM); ++i) {
      row[column_names[i]] = column;
    }
  }
  return content;
}

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

milliseconds parse_duration(std::string_view duration_str) {
  std::vector<std::string_view> chunks;
  chunks.reserve(3);
  uint64_t pos = 0;
  for (uint64_t i = 0; i < duration_str.size(); ++i) {
    if (duration_str[i] == ':') {
      chunks.push_back(duration_str.substr(pos, i - pos));
      pos = i + 1;
    }
  }
  if (pos < duration_str.size() - 1) chunks.push_back(duration_str.substr(pos));

  milliseconds duration{0};
  switch (chunks.size()) {
    case 3: { // Hours
      int64_t val;
      if (std::from_chars(
              chunks[0].data(), chunks[0].data() + chunks[0].size(), val)
              .ec == NO_ERROR) {
        duration += hours{val};
      }
      [[fallthrough]];
    }
    case 2: { // Minutes
      int i = chunks.size() - 2;
      int64_t val;
      if (std::from_chars(
              chunks[i].data(), chunks[i].data() + chunks[i].size(), val)
              .ec == NO_ERROR) {
        duration += minutes{val};
      }
      [[fallthrough]];
    }
    case 1: { // Seconds
      int i = chunks.size() - 1;
      double val;
      if (std::from_chars(
              chunks[i].data(), chunks[i].data() + chunks[i].size(), val)
              .ec == NO_ERROR) {
        duration += milliseconds{static_cast<int64_t>(val * 1000)};
      }
    }
  }
  return duration;
}

milliseconds parse_gap(std::string_view duration_str) {
  double val;
  if (duration_str.front() == '+') duration_str = duration_str.substr(1);
  if (std::from_chars(
          duration_str.data(), duration_str.data() + duration_str.size(), val)
          .ec != NO_ERROR) {
    std::cerr << "Failed to parse time gap: " << duration_str << std::endl;
    std::exit(1);
  }
  return milliseconds{static_cast<int64_t>(val * 1000)};
}

int parse_position(std::string_view position_str) {
  int position;
  if (std::from_chars(
          position_str.data(),
          position_str.data() + position_str.size(),
          position)
          .ec != NO_ERROR) {
    std::cerr << "Failed to parse race position: \"" << position_str << '"'
              << std::endl;
    std::exit(1);
  }
  return position;
}

f1_predict::constants::Team lookup_team(std::string_view team_name) {
  auto itr = NAME_TO_TEAM_MAP.find(team_name);
  if (itr == NAME_TO_TEAM_MAP.end()) {
    std::cerr << "Unknown team name: " << team_name << std::endl;
    std::exit(1);
  }
  return itr->second;
}

f1_predict::constants::Driver lookup_driver(std::string_view driver_name) {
  auto itr = NAME_TO_DRIVER_MAP.find(driver_name);
  if (itr == NAME_TO_DRIVER_MAP.end()) {
    std::cerr << "Unknown driver name: " << driver_name << std::endl;
    std::exit(1);
  }
  return itr->second;
}

google::protobuf::Duration to_proto_duration(milliseconds duration) {
  return TimeUtil::MillisecondsToDuration(duration.count());
}

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
          parse_position(result.at(STARTING_POSITION_COLUMN)));
    }
    if (!result.at(POSITION_COLUMN).empty() &&
        result.at(POSITION_COLUMN) != NC && result.at(POSITION_COLUMN) != DQ) {
      proto_result.set_final_position(
          parse_position(result.at(POSITION_COLUMN)));
    }
    if (proto_result.final_position() == 1) {
      *proto_result.mutable_finals_time() = to_proto_duration(fastest_time);
    } else if (
        result.at(TIME_COLUMN) != DNF && result.at(TIME_COLUMN) != DNS &&
        result.at(TIME_COLUMN) != DSQ) {
      *proto_result.mutable_finals_time() =
          to_proto_duration(fastest_time + parse_gap(result.at(TIME_COLUMN)));
    }

    std::string output;
    if (!TextFormat::PrintToString(proto_result, &output)) {
      std::cerr << "Failed to print out race results.";
      std::exit(1);
    }
    std::ofstream out_stream{out_path};
    out_stream << output << std::endl;
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

    std::string output;
    if (!TextFormat::PrintToString(proto_result, &output)) {
      std::cerr << "Failed to print out race results.";
      std::exit(1);
    }
    std::ofstream out_stream{out_path};
    out_stream << output << std::endl;
  }
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  fs::path input = absl::GetFlag(FLAGS_input_file);
  if (input.extension() != INPUT_EXTENSION) {
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

  auto data = load_input(input);
  if (data.empty()) {
    std::cerr << "No data loaded." << std::endl;
    return 1;
  }

  std::unordered_map<
      f1_predict::constants::Circuit,
      std::vector<std::unordered_map<std::string, std::string>>>
      races_to_results;
  for (const auto& row : data) {
    auto itr = NAME_TO_CIRCUIT_MAP.find(row.at(CIRCUIT_COLUMN));
    if (itr == NAME_TO_CIRCUIT_MAP.end()) {
      std::cerr << "Unknown circuit name: " << row.at(CIRCUIT_COLUMN)
                << std::endl;
      return 1;
    }
    races_to_results[itr->second].push_back(std::move(row));
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
