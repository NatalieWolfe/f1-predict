#include "data/csv.h"

#include <iostream>
#include <istream>
#include <sstream>
#include <string>
#include <system_error>
#include <unordered_map>
#include <vector>

#include "strings/trim.h"

namespace f1_predict {
namespace {

constexpr char DELIM = ',';

}

std::vector<std::unordered_map<std::string, std::string>>
load_csv(std::istream& input) {
  std::string header_line;
  if (!std::getline(input, header_line)) {
    std::cerr << "Input is empty." << std::endl;
    std::exit(1);
  }

  std::stringstream header_stream(header_line);
  std::vector<std::string> column_names;
  for (std::string column; std::getline(header_stream, column, DELIM);) {
    column_names.emplace_back(trim(column));
  }

  std::vector<std::unordered_map<std::string, std::string>> content;
  for (std::string line; std::getline(input, line);) {
    std::stringstream line_stream(line);
    auto& row = content.emplace_back();
    std::string column;
    for (int i = 0; std::getline(line_stream, column, DELIM); ++i) {
      row[column_names[i]] = trim(column);
    }
  }
  return content;
}

} // namespace f1_predict
