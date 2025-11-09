#include "model/writer.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <ostream>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "data/constants.pb.h"
#include "data/proto_utils.h"
#include "data/race_results.pb.h"
#include "google/protobuf/duration.pb.h"

namespace f1_predict {
namespace {

using ::google::protobuf::Duration;
using ::std::chrono::duration_cast;
using ::std::chrono::milliseconds;

constexpr int64_t DEFAULT_NUMBER = 999999999;
constexpr milliseconds DEFAULT_TIME{DEFAULT_NUMBER};
constexpr milliseconds ZERO_MS{0};
constexpr std::string NA = "NA";

struct aggregate_data {
  size_t race_id;
  size_t race_size;
  milliseconds best_qual_time;
  milliseconds worst_qual_time;
  milliseconds qual_spread;
  milliseconds median_qual_time;
};

struct result_data {
  size_t index;
  const DriverResult& driver;
};

milliseconds time_or_default(
    const Duration& duration, milliseconds default_value = DEFAULT_TIME) {
  milliseconds mils = to_milliseconds(duration);
  return mils.count() > 0 ? mils : default_value;
}

std::string time_or_na(const Duration& duration) {
  milliseconds ms = time_or_default(duration);
  return ms == DEFAULT_TIME ? NA : std::to_string(ms.count());
}

milliseconds best_qual_time(const DriverResult& result) {
  return std::min(
      {time_or_default(result.qualification_time_1()),
       time_or_default(result.qualification_time_2()),
       time_or_default(result.qualification_time_3())});
}

template <
    std::ranges::range Values,
    std::enable_if_t<!std::is_integral_v<std::ranges::range_value_t<Values>>>* =
        nullptr>
auto average(const Values& values) {
  using namespace std::ranges;
  return std::accumulate(begin(values), end(values), range_value_t<Values>{0}) /
      size(values);
}

template <
    std::ranges::range Values,
    std::enable_if_t<std::is_integral_v<std::ranges::range_value_t<Values>>>* =
        nullptr>
double average(const Values& values) {
  using namespace std::ranges;
  return static_cast<double>(std::accumulate(
             begin(values), end(values), range_value_t<Values>{0})) /
      size(values);
}

const historical_data::stats* find_historical_circuit_driver_data(
    const historical_data& data, const DriverResult& race) {
  auto circuit_itr = data.circuit_drivers.find(race.circuit());
  if (circuit_itr != data.circuit_drivers.end()) {
    auto drivers_itr = circuit_itr->second.find(race.driver());
    if (drivers_itr != circuit_itr->second.end()) return &drivers_itr->second;
  }
  return nullptr;
}

const historical_data::stats* find_historical_circuit_team_data(
    const historical_data& data, const DriverResult& race) {
  auto circuit_itr = data.circuit_teams.find(race.circuit());
  if (circuit_itr != data.circuit_teams.end()) {
    auto team_itr = circuit_itr->second.find(race.team());
    if (team_itr != circuit_itr->second.end()) return &team_itr->second;
  }
  return nullptr;
}

const historical_data::stats* find_historical_driver_career_data(
    const historical_data& data, const DriverResult& race) {
  auto driver_itr = data.driver_career.find(race.driver());
  if (driver_itr != data.driver_career.end()) { return &driver_itr->second; }
  return nullptr;
}

template <std::ranges::range Container>
auto last_n(Container&& container, int64_t n) {
  return std::ranges::subrange(
      std::ranges::begin(container) +
          std::max(0l, static_cast<int64_t>(std::ranges::size(container)) - n),
      std::ranges::end(container));
}

template <std::ranges::range Container>
double standard_deviation(const Container& container) {
  const auto size = std::ranges::size(container);
  if (size <= 1) { return 0.0; }
  double mean =
      std::accumulate(
          std::ranges::begin(container), std::ranges::end(container), 0.0) /
      size;
  double sq_diff_sum = std::accumulate(
      std::ranges::begin(container),
      std::ranges::end(container),
      0.0,
      [mean](double x, double v) {
        double diff = v - mean;
        return x + (diff * diff);
      });
  return std::sqrt(sq_diff_sum / size);
}

} // namespace

namespace writer_internal {

class column_writer {
public:
  column_writer() = default;
  virtual ~column_writer() = default;
  virtual void write_header(std::ostream& out) const = 0;
  virtual void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data& aggregate,
      const historical_data& historical) const = 0;
};

} // namespace writer_internal

namespace {

class relevance_label_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "relevance_label";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data& aggregate,
      const historical_data&) const override {
    out << (aggregate.race_size - result.index);
  }
};

class race_id_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "race_id"; }
  void write_column(
      std::ostream& out,
      const result_data&,
      const aggregate_data& aggregate,
      const historical_data&) const override {
    out << aggregate.race_id;
  }
};

class circuit_id_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "circuit_id"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data&) const override {
    out << result.driver.circuit();
  }
};

class season_id_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "season_id"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data&) const override {
    out << (result.driver.race_season() - 1900);
  }
};

class team_id_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "team_id"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data&) const override {
    out << result.driver.team();
  }
};

class driver_id_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "driver_id"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data&) const override {
    out << result.driver.driver();
  }
};

class qual_spread_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "qual_spread_msec";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data& aggregate,
      const historical_data&) const override {
    out << duration_cast<milliseconds>(
               aggregate.worst_qual_time - aggregate.best_qual_time)
               .count();
  }
};

class starting_position_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "starting_position";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data&) const override {
    out << result.driver.starting_position();
  }
};

class q1_time_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "q1_time_msec"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data&) const override {
    out << time_or_na(result.driver.qualification_time_1());
  }
};

class q2_time_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "q2_time_msec"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data&) const override {
    out << time_or_na(result.driver.qualification_time_2());
  }
};

class q3_time_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "q3_time_msec"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data&) const override {
    out << time_or_na(result.driver.qualification_time_3());
  }
};

class driver_best_qual_time_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "driver_best_qual_time_msec";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data&) const override {
    out << best_qual_time(result.driver).count();
  }
};

class gap_to_best_qual_time_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "gap_to_best_qual_time_msec";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data& aggregate,
      const historical_data&) const override {
    out << best_qual_time(result.driver).count() -
            duration_cast<milliseconds>(aggregate.best_qual_time).count();
  }
};

class gap_to_median_qual_time_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "gap_to_median_qual_time_msec";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data& aggregate,
      const historical_data&) const override {
    out << best_qual_time(result.driver).count() -
            duration_cast<milliseconds>(aggregate.median_qual_time).count();
  }
};

class qual_consistency_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "qual_consistency_stddev";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data&) const override {
    std::vector<double> qual_times;
    if (to_milliseconds(result.driver.qualification_time_1()) != ZERO_MS) {
      qual_times.push_back(
          to_milliseconds(result.driver.qualification_time_1()).count());
    }
    if (to_milliseconds(result.driver.qualification_time_2()) != ZERO_MS) {
      qual_times.push_back(
          to_milliseconds(result.driver.qualification_time_2()).count());
    }
    if (to_milliseconds(result.driver.qualification_time_3()) != ZERO_MS) {
      qual_times.push_back(
          to_milliseconds(result.driver.qualification_time_3()).count());
    }
    if (qual_times.size() <= 1) {
      out << NA;
      return;
    }
    double stddev = standard_deviation(qual_times);
    out << std::setprecision(6) << std::fixed
        << (std::isnan(stddev) ? 0.0 : stddev);
  }
};

class driver_average_result_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "driver_average_result";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data& historical) const override {
    const auto* driver_stats =
        find_historical_circuit_driver_data(historical, result.driver);
    if (driver_stats) {
      out << average(driver_stats->finals_positions);
    } else {
      out << NA;
    }
  }
};

class driver_circuit_result_stddev_column :
    public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "driver_circuit_result_stddev";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data& historical) const override {
    const auto* driver_stats =
        find_historical_circuit_driver_data(historical, result.driver);
    if (driver_stats && driver_stats->finals_positions.size() > 1) {
      out << standard_deviation(driver_stats->finals_positions);
    } else {
      out << NA;
    }
  }
};

class driver_recent_circuit_result_stddev_column :
    public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "driver_recent_circuit_result_stddev";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data& historical) const override {
    const auto* driver_stats =
        find_historical_circuit_driver_data(historical, result.driver);
    if (driver_stats && driver_stats->finals_positions.size() > 1) {
      out << standard_deviation(last_n(driver_stats->finals_positions, 3));
    } else {
      out << NA;
    }
  }
};

class driver_recent_average_result_column :
    public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "driver_recent_average_result";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data& historical) const override {
    const auto* driver_stats =
        find_historical_circuit_driver_data(historical, result.driver);
    if (driver_stats) {
      out << average(last_n(driver_stats->finals_positions, 3));
    } else {
      out << NA;
    }
  }
};

class driver_career_stddev_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "driver_career_stddev";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data& historical) const override {
    const auto* driver_stats =
        find_historical_driver_career_data(historical, result.driver);
    if (driver_stats && driver_stats->finals_positions.size() > 1) {
      out << standard_deviation(last_n(driver_stats->finals_positions, 3));
    } else {
      out << NA;
    }
  }
};

class team_average_result_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "team_average_result";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data& historical) const override {
    const auto* team_stats =
        find_historical_circuit_team_data(historical, result.driver);
    if (team_stats) {
      out << average(team_stats->finals_positions);
    } else {
      out << NA;
    }
  }
};

class team_recent_average_result_column :
    public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override {
    out << "team_recent_average_result";
  }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&,
      const historical_data& historical) const override {
    const auto* team_stats =
        find_historical_circuit_team_data(historical, result.driver);
    if (team_stats) {
      out << average(last_n(team_stats->finals_positions, 6));
    } else {
      out << NA;
    }
  }
};

template <typename... ColumnWriters>
std::vector<std::shared_ptr<writer_internal::column_writer>> make_columns() {
  std::vector<std::shared_ptr<writer_internal::column_writer>> columns;
  (columns.push_back(std::make_shared<ColumnWriters>()), ...);
  return columns;
}

} // namespace

writer::writer(std::filesystem::path output_path, writer_options opts)
    : _options{std::move(opts)}, _output_path{std::move(output_path)},
      _out{_output_path}, _columns{make_columns<
                              relevance_label_column,
                              race_id_column,
                              circuit_id_column,
                              season_id_column,
                              team_id_column,
                              driver_id_column,
                              qual_spread_column,
                              starting_position_column,
                              q1_time_column,
                              q2_time_column,
                              q3_time_column,
                              driver_best_qual_time_column,
                              gap_to_best_qual_time_column,
                              qual_consistency_column,
                              driver_average_result_column,
                              driver_recent_average_result_column,
                              driver_career_stddev_column,
                              team_average_result_column,
                              team_recent_average_result_column>()} {}

void writer::write_header() {
  int column_counter = 0;
  for (const auto& column : _columns) {
    if (++column_counter > 1) _out << _options.delim;
    column->write_header(_out);
  }
  _out << '\n' << std::flush;
}

void writer::write_race(
    std::span<const DriverResult> race_results,
    const historical_data& historical) {
  if (race_results.empty()) return;

  aggregate_data aggregate{
      .race_id = std::hash<std::string>{}(absl::StrCat(
          constants::Circuit_Name(race_results.front().circuit()),
          "_",
          race_results.front().race_season())),
      .race_size = std::min(_options.race_size_limit, race_results.size()),
      .best_qual_time = milliseconds::max()};

  std::vector<const DriverResult*> sorted_results;
  for (const DriverResult& result : race_results) {
    milliseconds driver_best_qual_time = best_qual_time(result);
    aggregate.best_qual_time =
        std::min(aggregate.best_qual_time, driver_best_qual_time);
    if (driver_best_qual_time != DEFAULT_TIME) {
      aggregate.worst_qual_time =
          std::max(aggregate.worst_qual_time, driver_best_qual_time);
    }
    sorted_results.push_back(&result);
  }

  std::ranges::sort(sorted_results, [](const auto* a, const auto* b) {
    if (a->final_position() == b->final_position()) {
      return a->starting_position() < b->starting_position();
    }
    return a->final_position() < b->final_position();
  });

  if (sorted_results.size() % 2 == 1) {
    aggregate.median_qual_time =
        best_qual_time(*sorted_results[sorted_results.size() / 2]);
  } else {
    aggregate.median_qual_time =
        (best_qual_time(*sorted_results[sorted_results.size() / 2]) +
         best_qual_time(*sorted_results[(sorted_results.size() / 2) - 1])) /
        2;
  }

  std::span<const f1_predict::DriverResult*> results_span{
      sorted_results.begin(), sorted_results.end()};
  if (results_span.size() > _options.race_size_limit) {
    results_span =
        results_span.subspan(results_span.size() - _options.race_size_limit);
  }

  for (size_t i = 0; i < results_span.size(); ++i) {
    size_t column_counter = 0;
    for (const auto& column : _columns) {
      if (++column_counter > 1) _out << _options.delim;
      column->write_column(
          _out,
          {.index = i, .driver = *results_span[i]},
          aggregate,
          historical);
    }
    _out << '\n';
  }
  _out << std::flush;
}

} // namespace f1_predict
