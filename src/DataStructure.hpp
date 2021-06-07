#pragma once

#include <fstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>
using std::ifstream;
using std::ofstream;
using std::string;
using std::tuple;
using std::unordered_map;
using std::vector;

namespace Config {
const int kBlockSize = 4096;
#ifdef _DEBUG
const int kMaxBlockNum = 2;
#else
const int kMaxBlockNum = 128;
#endif
}  // namespace Config

enum struct Operator { GT, GE, LT, LE, EQ, NE };

enum struct SqlValueTypeBase { Integer, Float, String };
typedef unsigned SqlValueType;  // if >= SqlValueTypeBase::String, it means
                                // char(val - SqlValueTypeBase::String)

struct SqlValue {
  SqlValueType type;
  union {
    int Integer;
    float Float;
    const char *String;
  } val;
  bool operator<(const SqlValue &rhs) const {
    if (type != rhs.type)
      throw std::invalid_argument("comparison between different SqlValueType");
    if (type == static_cast<SqlValueType>(SqlValueTypeBase::Integer)) {
      return val.Integer < rhs.val.Integer;
    } else if (type == static_cast<SqlValueType>(SqlValueTypeBase::Float)) {
      return val.Float < rhs.val.Float;
    } else {
      // TODO
      return val.String < rhs.val.String;
    }
  };
  bool operator==(const SqlValue &rhs) const {
    if (type != rhs.type)
      throw std::invalid_argument("comparison between different SqlValueType");
    if (type == static_cast<SqlValueType>(SqlValueTypeBase::Integer)) {
      return val.Integer == rhs.val.Integer;
    } else if (type == static_cast<SqlValueType>(SqlValueTypeBase::Float)) {
      return val.Float == rhs.val.Float;
    } else {
      // TODO
      return val.String == rhs.val.String;
    }
  }
  bool Compare(const Operator &op, const SqlValue &rhs) const {
    switch (op) {
      case Operator::GT:
        return rhs < *this;
      case Operator::GE:
        return rhs < *this || rhs == *this;
      case Operator::LT:
        return *this < rhs;
      case Operator::LE:
        return *this < rhs || *this == rhs;
      case Operator::EQ:
        return *this == rhs;
      case Operator::NE:
        return !(*this == rhs);
    }
  }
};

struct Tuple {
  vector<SqlValue> values;
};

enum struct SpecialAttribute { None, PrimaryKey, UniqueKey };

struct Table {
  string table_name;
  unordered_map<string, tuple<SqlValueType, SpecialAttribute, size_t>>
      attributes;  // size_t is the offset of the attribute in the record
  unordered_map<string, string> indexes;  // attribute name, index name
};

struct Block {
  char val_[Config::kBlockSize];
  bool dirty_, pin_;  // CAUTION: set dirty_ to true after modification
  Block() = default;
  virtual ~Block() = default;
  /**
   * @brief Get the filename of a block by block_id
   *
   * @param block_id the id of the block
   * @return the filename
   */
  static string GetBlockFilename(size_t block_id) {
    return std::to_string(block_id) + ".block";
  };
  /**
   * @brief read raw data from ifstream
   *
   * @param os the ifstream
   */
  void read(ifstream &is) { is.read(val_, Config::kBlockSize); }
  /**
   * @brief write raw data into ofstream
   *
   * @param os the ofstream
   */
  void write(ofstream &os) const { os.write(val_, Config::kBlockSize); }
};

struct Position {
  size_t block_id, offset;
};

struct Condition {
  string attribute;
  Operator op;
  SqlValue val;
};
