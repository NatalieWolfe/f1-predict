#include "strings/levenshtein.h"

#include "gtest/gtest.h"

namespace f1_predict {
namespace {

TEST(LevenshteinDistance, SameString) {
  EXPECT_EQ(levenshtein_distance("foo", "foo"), 0);
}

TEST(LevenshteinDistance, Substitution) {
  EXPECT_EQ(levenshtein_distance("foo", "fbo"), 1);
}

TEST(LevenshteinDistance, Transpose) {
  EXPECT_EQ(levenshtein_distance("foo", "ofo"), 2);
}

TEST(LevenshteinDistance, Insert) {
  EXPECT_EQ(levenshtein_distance("foo", "oo"), 1);
}

TEST(LevenshteinDistance, Delete) {
  EXPECT_EQ(levenshtein_distance("foo", "ofoo"), 1);
}

TEST(LevenshteinDistance, Complex) {
  EXPECT_EQ(levenshtein_distance("shore a bear", "horse is bare"), 6);
}

} // namespace
} // namespace f1_predict
