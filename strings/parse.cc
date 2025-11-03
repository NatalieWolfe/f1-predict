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
  milliseconds duration{0};
  int chunk_count = 0;
  int prev_chunk_start = duration_str.size();
  for (int64_t i = static_cast<int64_t>(duration_str.size()) - 1; i >= 0; --i) {
    if (i > 0 && duration_str[i] != ':') continue;

    std::string_view chunk = duration_str.substr(i, prev_chunk_start - i);
    if (chunk[0] == ':') chunk = chunk.substr(1);
    switch (++chunk_count) {
      case 3: { // Hours
        int64_t val;
        if (from_chars(chunk, val)) duration += std::chrono::hours{val};
        break;
      }
      case 2: { // Minutes
        int64_t val;
        if (from_chars(chunk, val)) duration += std::chrono::minutes{val};
        break;
      }
      case 1: { // Seconds
        double val;
        if (from_chars(chunk, val)) {
          duration += milliseconds{static_cast<int64_t>(val * 1000)};
        }
        break;
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
