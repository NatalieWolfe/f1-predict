#include "strings/trim.h"

#include <string>
#include <string_view>

#include "gtest/gtest.h"

namespace f1_predict {
namespace {

TEST(Trim, EmptyString) {
  EXPECT_EQ(trim(""), "");
  EXPECT_EQ(trim(std::string()), "");
  EXPECT_EQ(trim(std::string_view()), "");
}

TEST(Trim, TrimsFront) {
  EXPECT_EQ(trim("  \t\r\nfront"), "front");
  EXPECT_EQ(trim(std::string("  \t\r\nfront")), "front");
  EXPECT_EQ(trim(std::string_view("  \t\r\nfront")), "front");
}

TEST(Trim, TrimsBack) {
  EXPECT_EQ(trim("back  \t\r\n"), "back");
  EXPECT_EQ(trim(std::string("back  \t\r\n")), "back");
  EXPECT_EQ(trim(std::string_view("back  \t\r\n")), "back");
}

TEST(Trim, TrimsBoth) {
  EXPECT_EQ(trim("  \t\r\nboth \t\r\n  "), "both");
  EXPECT_EQ(trim(std::string("  \t\r\nboth \t\r\n  ")), "both");
  EXPECT_EQ(trim(std::string_view("  \t\r\nboth \t\r\n  ")), "both");
}

} // namespace
} // namespace f1_predict
