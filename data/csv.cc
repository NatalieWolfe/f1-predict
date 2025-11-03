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

std::vector<std::string> readline(std::istream& input) {
  std::vector<std::string> row;
  row.emplace_back();
  char c;
  bool in_quotes = false;
  while (input.get(c)) {
    if (!in_quotes && c == ',') {
      row.emplace_back();
      continue;
    }
    if (c == '"') {
      in_quotes = !in_quotes;
      continue;
    }
    if (c == '\n') break;
    if (c == '\\') input >> c;
    row.back().push_back(c);
  }
  if (row.size() == 1 && row.front() == "") return {};
  return row;
}

} // namespace

std::vector<std::unordered_map<std::string, std::string>>
load_csv(std::istream& input) {
  std::vector<std::string> column_names = readline(input);
  if (column_names.empty()) {
    std::cerr << "Input is empty." << std::endl;
    std::exit(1);
  }

  std::vector<std::unordered_map<std::string, std::string>> content;
  for (std::vector<std::string> line; line = readline(input), !line.empty();) {
    auto& row = content.emplace_back();
    for (int i = 0; i < line.size(); ++i) {
      row[column_names[i]] = trim(line[i]);
    }
  }
  return content;
}

} // namespace f1_predict
