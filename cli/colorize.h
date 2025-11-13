#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace f1_predict {

enum class color : uint8_t {
  COLOR_UNSPECIFIED = 0,
  BLACK,
  GRAY,
  WHITE,
  RED,
  GREEN,
  BLUE,
  CYAN,
  MAGENTA,
  YELLOW
};

// Wraps the given string in terminal color escape sequences.
std::string colorize(std::string_view str, color c);

} // namespace f1_predict
