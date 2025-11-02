#pragma once

#include <string_view>
#include <unordered_map>

#include "data/constants.pb.h"

namespace f1_predict {

extern const std::unordered_map<std::string_view, constants::Circuit>
    NAME_TO_CIRCUIT_MAP;

extern const std::unordered_map<std::string_view, constants::Team>
    NAME_TO_TEAM_MAP;

extern const std::unordered_map<std::string_view, constants::Driver>
    NAME_TO_DRIVER_MAP;

} // namespace f1_predict
