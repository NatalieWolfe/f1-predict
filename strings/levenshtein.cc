#include "strings/levenshtein.h"

#include <algorithm>
#include <string_view>
#include <vector>

namespace f1_predict {

int levenshtein_distance(std::string_view a, std::string_view b) {
  // Create a 2D vector (matrix) to store the distances
  std::vector<std::vector<int>> distances(
      a.size() + 1, std::vector<int>(b.size() + 1));

  // Initialize the first row and column
  // distances[i][0] represents the distance to transform an empty string to the
  // first i characters of a (i deletions)
  for (size_t i = 0; i <= a.size(); ++i) {
    distances[i][0] = i;
  }
  // distances[0][j] represents the distance to transform the first j characters
  // of b to an empty string (j insertions)
  for (size_t j = 0; j <= b.size(); ++j) {
    distances[0][j] = j;
  }

  // Fill the rest of the matrix
  for (size_t i = 1; i <= a.size(); ++i) {
    for (size_t j = 1; j <= b.size(); ++j) {
      if (a[i - 1] == b[j - 1]) {
        // Characters match, no operation needed for this step
        distances[i][j] = distances[i - 1][j - 1];
      } else {
        // Characters don't match, consider insertion, deletion, or substitution
        distances[i][j] = std::min(
                              {distances[i - 1][j],        // Deletion
                               distances[i][j - 1],        // Insertion
                               distances[i - 1][j - 1]}) + // Substitution
            1;
      }
    }
  }

  // The bottom-right cell contains the Levenshtein distance
  return distances[a.size()][b.size()];
}

} // namespace f1_predict
