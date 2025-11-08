#pragma once

#include <unordered_map>
#include <vector>

#include "data/constants.pb.h"

namespace f1_predict {

struct historical_data {
  struct circuit_stats {
    std::vector<int> finals_positions;
  };
  std::unordered_map<
      constants::Circuit,
      std::unordered_map<constants::Driver, circuit_stats>>
      circuit_drivers;
  std::unordered_map<
      constants::Circuit,
      std::unordered_map<constants::Team, circuit_stats>>
      circuit_teams;
};

} // namespace f1_predict
