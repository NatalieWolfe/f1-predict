#pragma once

#include <string_view>

#include "data/constants.pb.h"

namespace f1_predict {

constants::Circuit lookup_circuit(std::string_view circuit_name);
constants::Driver lookup_driver(std::string_view driver_name);
constants::Team lookup_team(std::string_view team_name);

} // namespace f1_predict
