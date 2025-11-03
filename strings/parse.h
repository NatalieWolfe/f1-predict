#pragma once

#include <chrono>
#include <string_view>

namespace f1_predict {

std::chrono::milliseconds parse_duration(std::string_view duration_str);

std::chrono::milliseconds parse_gap(std::string_view duration_str);

int parse_int(std::string_view int_str);

} // namespace f1_predict
