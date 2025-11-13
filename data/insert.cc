#include <cctype>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <limits>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_cat.h"
#include "cli/autocomplete.h"
#include "data/constants.pb.h"
#include "data/proto_utils.h"
#include "data/race_results.pb.h"
#include "google/protobuf/descriptor.h"
#include "strings/parse.h"

ABSL_FLAG(
    std::string,
    results_dir,
    "",
    "Directory where race results data are stored.");

namespace fs = ::std::filesystem;
namespace f1_c = ::f1_predict::constants;

using ::f1_predict::DriverResult;
using ::f1_predict::parse_duration;
using ::f1_predict::parse_int;
using ::f1_predict::select_from_list;
using ::f1_predict::to_milliseconds;
using ::f1_predict::to_proto_duration;
using ::std::chrono::duration_cast;
using ::std::chrono::hours;
using ::std::chrono::milliseconds;
using ::std::chrono::minutes;

char char_to_upper(char c) { return std::toupper(c); }
bool char_is_digit(char c) { return std::isdigit(c); }

template <typename Enum>
Enum select(const google::protobuf::EnumDescriptor& descriptor) {
  std::vector<std::string> names;
  names.reserve(descriptor.value_count());
  for (int i = 0; i < descriptor.value_count(); ++i) {
    const auto* val_descriptor = descriptor.value(i);
    if (val_descriptor && val_descriptor->number() != 0) {
      names.emplace_back(val_descriptor->name());
    }
  }

  auto maybe_selection = select_from_list(names);
  if (maybe_selection) {
    return static_cast<Enum>(
        descriptor.FindValueByName(*maybe_selection)->number());
  }
  return static_cast<Enum>(0);
}

int prompt_int(int min, int max) {
  int i = std::numeric_limits<int>::min();
  while (i < min || i > max) {
    std::cout << "> ";
    std::cin >> i;
    std::cin.ignore();
  }
  return i;
}

std::string format_duration(milliseconds duration) {
  using namespace std::chrono_literals;
  std::string str;
  hours h = duration_cast<hours>(duration);
  if (h > 0h) {
    str = absl::StrCat(h.count(), ":");
    duration -= h;
  }
  minutes m = duration_cast<minutes>(duration);
  if (m > 0min || !str.empty()) {
    absl::StrAppend(&str, m.count(), ":");
    duration -= m;
  }
  absl::StrAppend(&str, duration.count() / 1000.0);
  return str;
}

std::string format_duration(const google::protobuf::Duration& duration) {
  return format_duration(to_milliseconds(duration));
}

milliseconds prompt_duration() {
  milliseconds duration{0};
  std::cout << "> ";
  std::string input;
  std::getline(std::cin, input);
  return parse_duration(input);
}

void prompt_duration_field(
    std::string_view msg, DriverResult& result, std::string_view field_name) {
  std::cout << msg;

  const auto& field_descriptor =
      *DriverResult::descriptor()->FindFieldByName(field_name);
  const auto& result_reflection = *DriverResult::GetReflection();
  if (result_reflection.HasField(result, &field_descriptor)) {
    std::cout << " ("
              << format_duration(
                     static_cast<const google::protobuf::Duration&>(
                         result_reflection.GetMessage(
                             result, &field_descriptor)))
              << ")";
  }
  std::cout << "\n";
  milliseconds duration = prompt_duration();
  if (duration > milliseconds(0)) {
    static_cast<google::protobuf::Duration&>(*result_reflection.MutableMessage(
        &result, &field_descriptor)) = to_proto_duration(duration);
  }
}

bool prompt_bool() {
  do {
    std::string line;
    std::cout << "> ";
    std::getline(std::cin, line);
    if (line.empty() || line[0] == 'y' || line[0] == 'Y') return true;
    if (line[0] == 'n' || line[0] == 'N') return false;
  } while (true);
}

void prompt_qual_fields(DriverResult& results) {
  prompt_duration_field("Qual 1", results, "qualification_time_1");
  prompt_duration_field("Qual 2", results, "qualification_time_2");
  prompt_duration_field("Qual 3", results, "qualification_time_3");
  std::cout << "Starting position\n";
  results.set_starting_position(prompt_int(1, 50));
}

void prompt_finals_fields(DriverResult& results) {
  prompt_duration_field("Finals time", results, "finals_time");
  std::cout << "Final position\n";
  results.set_final_position(prompt_int(1, 50));
  std::cout << "Finals lap count\n";
  results.set_finals_lap_count(prompt_int(0, 200));
  prompt_duration_field(
      "Fastest finals lap time", results, "finals_fastest_lap_time");
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  const fs::path results_dir = absl::GetFlag(FLAGS_results_dir);
  if (results_dir.empty()) {
    std::cerr << "Must specify --results_dir flag." << std::endl;
    return 1;
  }

  std::cout << "Season\n";
  int season = prompt_int(1950, 2030);
  std::cout << "Circuit\n";
  auto circuit = select<f1_c::Circuit>(*f1_c::Circuit_descriptor());
  std::cout << "Driver\n";
  auto driver = select<f1_c::Driver>(*f1_c::Driver_descriptor());

  if (!circuit || !driver) {
    std::cerr << "Must specify circuit and driver." << std::endl;
    return 1;
  }

  fs::path results_file = results_dir / std::to_string(season) /
      f1_c::Circuit_Name(circuit) / f1_c::Driver_Name(driver);
  results_file += ".textproto";
  std::cout << results_file << ": ";
  f1_predict::DriverResult results;
  if (!fs::exists(results_file)) {
    std::cout << "Creating new results." << std::endl;
    fs::create_directories(results_file.parent_path());
    results.set_race_season(season);
    results.set_circuit(circuit);
    results.set_driver(driver);
  } else {
    std::cout << "Editing existing results." << std::endl;
    results = f1_predict::load_result(results_file);
  }

  std::cout << "Team";
  if (results.team()) {
    std::cout << " (" << f1_c::Team_Name(results.team()) << ")";
  }
  std::cout << "\n";
  results.set_team(select<f1_c::Team>(*f1_c::Team_descriptor()));

  std::cout << "Update qual? [Y/n]\n";
  if (prompt_bool()) prompt_qual_fields(results);

  std::cout << "Update finals? [Y/n]\n";
  if (prompt_bool()) prompt_finals_fields(results);

  std::cout << "Saving to " << results_file << "...";
  f1_predict::save_result(results_file, results);
  std::cout << "\n";

  return 0;
}
