#include "strings/parse.h"

#include <chrono>

#include "gtest/gtest.h"

namespace f1_predict {
namespace {

using std::chrono_literals::operator""min;
using std::chrono_literals::operator""ms;
using std::chrono_literals::operator""h;

// MARK: parse_duration

TEST(ParseDuration, ParsesMilliseconds) {
  EXPECT_EQ(parse_duration("123"), 123000ms);
  EXPECT_EQ(parse_duration("123.456"), 123456ms);
  EXPECT_EQ(parse_duration("123.456789"), 123456ms);

  EXPECT_EQ(parse_duration(":123.456"), 123456ms);
  EXPECT_EQ(parse_duration("::123.456"), 123456ms);
}

TEST(ParseDuration, ParsesMinutes) {
  EXPECT_EQ(parse_duration("1:23"), 1min + 23000ms);
  EXPECT_EQ(parse_duration("12:3"), 12min + 3000ms);
  EXPECT_EQ(parse_duration("12:03"), 12min + 3000ms);
  EXPECT_EQ(parse_duration("1:55.018"), 1min + 55018ms);

  EXPECT_EQ(parse_duration("1:"), 1min);
  EXPECT_EQ(parse_duration(":1:23"), 1min + 23000ms);
}

TEST(ParseDuration, ParsesHours) {
  EXPECT_EQ(parse_duration("1:2:3"), 1h + 2min + 3000ms);
  EXPECT_EQ(parse_duration("12:34:56.789"), 12h + 34min + 56789ms);

  EXPECT_EQ(parse_duration("1:2:"), 1h + 2min);
  EXPECT_EQ(parse_duration("1::"), 1h);
  EXPECT_EQ(parse_duration("1::3"), 1h + 3000ms);
}

// MARK: parse_gap

TEST(ParseGap, ParsesInteger) { EXPECT_EQ(parse_gap("1"), 1000ms); }
TEST(ParseGap, ParsesFloat) { EXPECT_EQ(parse_gap("1.234"), 1234ms); }
TEST(ParseGap, ParsesPlusPrefix) { EXPECT_EQ(parse_gap("+1.234"), 1234ms); }

// MARK: parse_int

TEST(ParseInt, ParsesInteger) { EXPECT_EQ(parse_int("1"), 1); }

} // namespace
} // namespace f1_predict
