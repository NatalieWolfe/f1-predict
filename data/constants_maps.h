#pragma once

#include <string_view>

#include "data/constants.pb.h"

namespace f1_predict {

constants::Circuit lookup_circuit(std::string_view circuit_name);
constants::Driver lookup_driver(std::string_view driver_name);
constants::Team lookup_team(std::string_view team_name);
constants::Circuit lookup_circuit_id(std::string_view circuit_id);
constants::Driver lookup_driver_id(std::string_view driver_id);
constants::Team lookup_team_id(std::string_view team_id);

} // namespace f1_predict
