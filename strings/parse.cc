#include "strings/parse.h"

#include <charconv>
#include <chrono>
#include <iostream>
#include <string_view>
#include <system_error>

namespace f1_predict {
namespace {

using ::std::chrono::milliseconds;

constexpr std::errc NO_ERROR{};

template <typename T>
bool from_chars(std::string_view str, T& val) {
  return std::from_chars(str.data(), str.data() + str.size(), val).ec ==
      NO_ERROR;
}

} // namespace

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
      if (from_chars(chunks[0], val)) duration += std::chrono::hours{val};
      [[fallthrough]];
    }
    case 2: { // Minutes
      int i = chunks.size() - 2;
      int64_t val;
      if (from_chars(chunks[i], val)) duration += std::chrono::minutes{val};
      [[fallthrough]];
    }
    case 1: { // Seconds
      int i = chunks.size() - 1;
      double val;
      if (from_chars(chunks[i], val)) {
        duration += milliseconds{static_cast<int64_t>(val * 1000)};
      }
    }
  }
  return duration;
}

milliseconds parse_gap(std::string_view duration_str) {
  double val;
  if (duration_str.front() == '+') duration_str = duration_str.substr(1);
  if (!from_chars(duration_str, val)) {
    std::cerr << "Failed to parse time gap: " << duration_str << std::endl;
    std::exit(1);
  }
  return milliseconds{static_cast<int64_t>(val * 1000)};
}

int parse_int(std::string_view int_str) {
  int i;
  if (!from_chars(int_str, i)) {
    std::cerr << "Failed to parse integer: \"" << int_str << '"' << std::endl;
    std::exit(1);
  }
  return i;
}

} // namespace f1_predict
