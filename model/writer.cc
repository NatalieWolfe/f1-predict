#include "model/writer.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <ostream>
#include <span>
#include <string>
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

constexpr milliseconds DEFAULT_TIME{9999999};

struct aggregate_data {
  size_t race_id;
  size_t race_size;
  milliseconds best_qual_time;
};

struct result_data {
  size_t index;
  const DriverResult& driver;
};

milliseconds time_or_default(const Duration& duration) {
  milliseconds mils = to_milliseconds(duration);
  return mils.count() > 0 ? mils : DEFAULT_TIME;
}

milliseconds best_qual_time(const DriverResult& result) {
  return std::min(
      {time_or_default(result.qualification_time_1()),
       time_or_default(result.qualification_time_2()),
       time_or_default(result.qualification_time_3())});
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
      const aggregate_data& aggregate) const = 0;
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
      const aggregate_data& aggregate) const override {
    out << (aggregate.race_size - result.index);
  }
};

class race_id_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "race_id"; }
  void write_column(
      std::ostream& out,
      const result_data&,
      const aggregate_data& aggregate) const override {
    out << aggregate.race_id;
  }
};

class circuit_id_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "circuit_id"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&) const override {
    out << result.driver.circuit();
  }
};

class season_id_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "season_id"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&) const override {
    out << (result.driver.race_season() - 1900);
  }
};

class team_id_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "team_id"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&) const override {
    out << result.driver.team();
  }
};

class driver_id_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "driver_id"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&) const override {
    out << result.driver.driver();
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
      const aggregate_data&) const override {
    out << result.driver.starting_position();
  }
};

class q1_time_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "q1_time_msec"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&) const override {
    out << time_or_default(result.driver.qualification_time_1()).count();
  }
};

class q2_time_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "q2_time_msec"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&) const override {
    out << time_or_default(result.driver.qualification_time_2()).count();
  }
};

class q3_time_column : public writer_internal::column_writer {
public:
  void write_header(std::ostream& out) const override { out << "q3_time_msec"; }
  void write_column(
      std::ostream& out,
      const result_data& result,
      const aggregate_data&) const override {
    out << time_or_default(result.driver.qualification_time_3()).count();
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
      const aggregate_data&) const override {
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
      const aggregate_data& aggregate) const override {
    out << best_qual_time(result.driver).count() -
            duration_cast<milliseconds>(aggregate.best_qual_time).count();
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
                              starting_position_column,
                              q1_time_column,
                              q2_time_column,
                              q3_time_column,
                              driver_best_qual_time_column,
                              gap_to_best_qual_time_column>()} {}

void writer::write_header() {
  for (const auto& column : _columns) {
    column->write_header(_out);
    _out << _options.delim;
  }
  _out << '\n' << std::flush;
}

void writer::write_race(std::span<const DriverResult> race_results) {
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
    aggregate.best_qual_time =
        std::min(aggregate.best_qual_time, best_qual_time(result));
    sorted_results.push_back(&result);
  }

  std::ranges::sort(sorted_results, [](const auto* a, const auto* b) {
    if (a->final_position() == b->final_position()) {
      return a->starting_position() < b->starting_position();
    }
    return a->final_position() < b->final_position();
  });
  std::span<const f1_predict::DriverResult*> results_span{
      sorted_results.begin(), sorted_results.end()};
  if (results_span.size() > _options.race_size_limit) {
    results_span =
        results_span.subspan(results_span.size() - _options.race_size_limit);
  }

  for (size_t i = 0; i < results_span.size(); ++i) {
    for (const auto& column : _columns) {
      column->write_column(
          _out, {.index = i, .driver = *results_span[i]}, aggregate);
      _out << _options.delim;
    }
    _out << '\n';
  }
  _out << std::flush;
}

} // namespace f1_predict
