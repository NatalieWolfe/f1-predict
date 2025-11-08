#pragma once

#include <unordered_map>
#include <vector>

#include "data/constants.pb.h"

namespace f1_predict {

struct historical_data {
  struct stats {
    std::vector<int> finals_positions;
  };
  std::unordered_map<
      constants::Circuit,
      std::unordered_map<constants::Driver, stats>>
      circuit_drivers;
  std::unordered_map<
      constants::Circuit,
      std::unordered_map<constants::Team, stats>>
      circuit_teams;
  std::unordered_map<constants::Driver, stats> driver_career;
};

} // namespace f1_predict
