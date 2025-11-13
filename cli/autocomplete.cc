#include "cli/autocomplete.h"

#include <asm-generic/ioctls.h>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <sys/ioctl.h>
#include <system_error>
#include <termios.h>
#include <unistd.h>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "absl/strings/str_cat.h"
#include "cli/colorize.h"
#include "strings/levenshtein.h"

namespace f1_predict {
namespace {

// Contains a single keypress input or character.
class input_t {
public:
  // Escape characters (incomplete).
  enum escape {
    NUL = 0x00,
    START_OF_HEADING = 0x01,
    START_OF_TEXT = 0x02,
    END_OF_TEXT = 0x03,
    END_OF_TRANSMISSION = 0x04,
    ENQUIRY = 0x05,
    ACKNOWLEDGE = 0x06,
    BELL = 0x07,
    BACKSPACE = 0x08,
    TAB = 0x09,
    LINE_FEED = 0x0a,
    VERTICAL_TAB = 0x0b,
    NEW_PAGE = 0x0c,
    CARRIAGE_RETURN = 0x0d,
    SHIFT_OUT = 0x0e,
    SHIFT_IN = 0x0f,
    ESCAPE = 0x1b, // 27 / 033 -> ESC character
    DELETE = 0x7f,

    ARROW_UP = 0x001b5b41,
    ARROW_DOWN = 0x001b5b42,
    ARROW_RIGHT = 0x001b5b43,
    ARROW_LEFT = 0x001b5b44
  };

  input_t() : value_{NUL} {}
  input_t(const input_t&) = default;
  input_t(input_t&&) = default;
  ~input_t() = default;
  input_t& operator=(const input_t&) = default;
  input_t& operator=(input_t&&) = default;
  bool operator==(const input_t&) const = default;

  explicit input_t(escape esc) : value_{esc} {}
  explicit input_t(char c) : value_{static_cast<char32_t>(c)} {}
  explicit input_t(char32_t c) : value_{c} {}

  // True if the input is a non-printable character.
  bool is_escape() const { return std::holds_alternative<escape>(value_); }

  // True if the input is a printable character.
  bool is_character() const { return std::holds_alternative<char32_t>(value_); }

  // The escape character if the input is an escape.
  //
  // Crashes if the input is not an escape.
  escape as_escape() const { return std::get<input_t::escape>(value_); }

  // Returns the character encoded as UTF-32 if the input is a character.
  //
  // Crashes if the input is not a character.
  char32_t as_character() const { return std::get<char32_t>(value_); }

private:
  std::variant<escape, char32_t> value_;
};

bool operator==(const input_t& input, input_t::escape esc) {
  return input.is_escape() && input.as_escape() == esc;
}

// Takes over stdout and stdin and provides a raw interface to the terminal.
//
// To safely release the terminal, Reset should be called before the object is
// destroyed.
class raw_io {
public:
  // Creates a new raw_io object.
  //
  // Upon destruction, terminal attributes are restored to their state at the
  // time of creation.
  static raw_io create() {
    auto attributes = std::make_unique<termios>();
    if (tcgetattr(STDOUT_FILENO, attributes.get())) {
      std::cerr << "tcgetattr failed." << std::endl;
      std::exit(1);
    }
    raw_io raw_io(std::move(attributes));
    raw_io._make_raw();
    return raw_io;
  }

  raw_io(const raw_io&) = delete;
  raw_io& operator=(const raw_io&) = delete;

  raw_io(raw_io&&) = default;
  raw_io& operator=(raw_io&&) = default;

  ~raw_io() {
    // Reset should be done safely before the object is destroyed. This is just
    // a cleanup to ensure the terminal is set correctly.
    if (_original_attributes != nullptr) {
      std::cerr << "RawIo not reset before destruction.This is unsafe."
                << std::endl;
      reset();
    }
  }

  // Removes characters printed on the cursor's line.
  void clear_line() const {
    constexpr std::string_view clear_sequence = "\33[2K\r";
    print(clear_sequence);
  }

  // Resets the terminal to the original attributes. The object should not be
  // used after this call.
  void reset() {
    if (_original_attributes == nullptr) return;
    if (tcsetattr(_fd, TCSAFLUSH, _original_attributes.get())) {
      std::cerr << "tcsetattr failed during reset" << std::endl;
      std::exit(1);
    }

    _original_attributes = nullptr;
  }

  // Writes the string to stdout.
  void print(std::string_view str) const { write(_fd, str.data(), str.size()); }

  // Reads a single input from stdin.
  std::optional<input_t> read() const {
    char buff[16] = {0};
    size_t bytes_read = ::read(_fd, &buff, sizeof(buff));
    if (bytes_read == 0) return std::nullopt;
    if (bytes_read == 1) {
      return std::isprint(buff[0])
          ? input_t(buff[0])
          : input_t(static_cast<input_t::escape>(buff[0]));
    }
    if (buff[0] == input_t::ESCAPE) {
      char esc[sizeof(input_t::escape)] = {0};
      for (std::size_t i = 0; i < bytes_read && i < sizeof(input_t::escape);
           ++i) {
        esc[bytes_read - i - 1] = buff[i];
      }
      return input_t{*reinterpret_cast<input_t::escape*>(esc)};
    }
    // UTF support is not implemented yet, but should be added here.
    return std::nullopt;
  }

private:
  explicit raw_io(std::unique_ptr<termios> original_attributes)
      : _original_attributes(std::move(original_attributes)) {}

  // Changes the terminal settings to unbuffered input and output and disables
  // echoing.
  void _make_raw() {
    termios raw_attributes{*_original_attributes};
    cfmakeraw(&raw_attributes);
    if (tcsetattr(STDOUT_FILENO, TCSAFLUSH, &raw_attributes)) {
      std::cerr << "tcsetattr failed to set raw mode" << std::endl;
      std::exit(1);
    }
  }

  std::unique_ptr<termios> _original_attributes;
  int _fd = STDOUT_FILENO;
};

// Returns the set of characters that should be omitted from querying the
// autocomplete list.
bool is_omitted_character(char32_t c) {
  static const std::unordered_set<char32_t> omitted_chars({' ', '\t'});
  return omitted_chars.contains(c);
}

// Returns the items that match the query in the order of their match distance.
std::vector<std::string_view>
get_matches(const std::vector<std::string>& items, std::string_view query) {
  std::string regex_str;
  regex_str.reserve((query.length() * 3) + 4);
  for (const char c : query) {
    if (is_omitted_character(c)) continue;
    absl::StrAppend(&regex_str, std::string_view(&c, 1), ".*");
  }

  namespace re_c = ::std::regex_constants;
  std::regex re{regex_str, re_c::icase | re_c::ECMAScript};

  std::vector<std::string_view> matches =
      items | std::views::filter([&](std::string_view str) {
        return std::regex_match(str.begin(), str.end(), re);
      }) |
      std::ranges::to<std::vector<std::string_view>>();
  std::sort(
      matches.begin(),
      matches.end(),
      [query](std::string_view a, std::string_view b) {
        return levenshtein_distance(query, a) < levenshtein_distance(query, b);
      });

  return matches;
}

// Returns the selection highlighted with the matching characters in query
// white.
std::string
highlight_match(std::string_view selection, std::string_view query) {
  std::string highlighted;
  for (int64_t i = 0, q = 0; i < static_cast<int64_t>(selection.size()); ++i) {
    if (q < static_cast<int64_t>(query.size()) &&
        std::tolower(selection[i]) == std::tolower(query[q])) {
      absl::StrAppend(&highlighted, std::string_view(&selection[i], 1));
      ++q;
    } else {
      absl::StrAppend(
          &highlighted,
          colorize(std::string_view(&selection[i], 1), color::GRAY));
    }
  }
  return highlighted;
}

bool update_query(std::string& query, int64_t& selection, const raw_io& io) {
  std::optional<input_t> c = io.read();
  if (!c) return false;
  if (c == input_t::BACKSPACE || c == input_t::DELETE) {
    if (!query.empty()) query.pop_back();
    return true;
  }
  if (c == input_t::ARROW_UP) {
    if (selection > 0) --selection;
    return true;
  }
  if (c == input_t::ARROW_DOWN) {
    ++selection;
    return true;
  }

  if (c->is_character()) {
    if (!is_omitted_character(c->as_character())) {
      selection = 0;
      query += static_cast<char>(c->as_character());
    }
    return true;
  }
  if (c != input_t::TAB && c != input_t::CARRIAGE_RETURN) { query = ""; }
  return false;
}

} // namespace

std::optional<std::string_view> autocomplete_select::select() const {
  raw_io io = raw_io::create();
  std::string query;
  std::vector<std::string_view> matches;
  int64_t selection = 0;

  try {
    do {
      io.clear_line();
      io.print("> ");

      if (query.empty()) continue;
      matches = get_matches(_items, query);
      if (matches.empty()) {
        io.print(colorize(query, color::RED));
        io.print(colorize(" (no match)", color::GRAY));
        continue;
      }

      if (selection >= static_cast<int64_t>(matches.size())) {
        selection = static_cast<int64_t>(matches.size()) - 1;
      }

      io.print(highlight_match(matches[selection], query));
    } while (update_query(query, selection, io));
  } catch (...) {
    io.reset();
    std::rethrow_exception(std::current_exception());
  }

  io.reset();

  if (query.empty() || matches.empty()) {
    std::cout << '\n';
    return std::nullopt;
  }
  std::string_view match = matches[selection];
  std::cout << "\r> " << match << '\n';
  return match;
}

std::optional<std::string_view>
select_from_list(const std::vector<std::string>& items) {
  autocomplete_select select(items);
  return select.select();
}

} // namespace f1_predict
