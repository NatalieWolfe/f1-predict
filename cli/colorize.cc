#include "cli/colorize.h"

#include <string>
#include <string_view>

#include "absl/strings/str_cat.h"

namespace f1_predict {
namespace {

constexpr std::string_view terminal_color_start(color c) {
  switch (c) {
    case color::BLACK:
      return "\033[30m";
    case color::RED:
      return "\033[31m";
    case color::GREEN:
      return "\033[32m";
    case color::YELLOW:
      return "\033[33m";
    case color::BLUE:
      return "\033[34m";
    case color::MAGENTA:
      return "\033[35m";
    case color::CYAN:
      return "\033[36m";
    case color::WHITE:
      return "\033[37m";
    case color::GRAY:
      return "\033[90m";
    case color::COLOR_UNSPECIFIED:
    default:
      return "\033[0m";
  }
}

} // namespace

std::string colorize(std::string_view str, color c) {
  return absl::StrCat(terminal_color_start(c), str, "\033[0m");
}

} // namespace f1_predict
