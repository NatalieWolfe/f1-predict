#pragma once

#include <string_view>

namespace f1_predict {

int levenshtein_distance(std::string_view a, std::string_view b);

} // namespace f1_predict
