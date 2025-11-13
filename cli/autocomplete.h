#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace f1_predict {

class autocomplete_select {
public:
  explicit autocomplete_select(const std::vector<std::string>& items)
      : _items{items} {}

  std::optional<std::string_view> select() const;

private:
  const std::vector<std::string>& _items;
};

std::optional<std::string_view>
select_from_list(const std::vector<std::string>& items);

} // namespace f1_predict
