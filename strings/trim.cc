#include "strings/trim.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>

namespace f1_predict {
namespace {

int64_t find_first_non_space(std::string_view str) {
  return std::ranges::find_if_not(str, [](char c) { return std::isspace(c); }) -
      str.begin();
}

int64_t find_last_non_space(std::string_view str) {
  return std::ranges::find_last_if_not(
             str, [](char c) { return std::isspace(c); })
             .begin() -
      str.begin();
}

} // namespace

std::string_view trim(std::string_view term) {
  int64_t start_pos = find_first_non_space(term);
  int64_t end_pos = find_last_non_space(term);
  int64_t len = end_pos - start_pos + 1;
  return term.substr(start_pos, len);
}

} // namespace f1_predict
