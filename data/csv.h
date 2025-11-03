#pragma once

#include <istream>
#include <string>
#include <unordered_map>
#include <vector>

namespace f1_predict {

std::vector<std::unordered_map<std::string, std::string>>
load_csv(std::istream& input);

}
