#pragma once

#include <cstring>
#include <fstream>
#include <map>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>
using std::ifstream;
using std::map;
using std::ofstream;
using std::string;
using std::tuple;
using std::vector;

namespace Config {
const int kMaxStringLength = 256;
const int kBlockSize = 4096;
#ifdef _DEBUG
const int kMaxBlockNum = 10;
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
    char String[Config::kMaxStringLength];
  } val;
  bool operator<(const SqlValue &rhs) const {
    if (type != rhs.type)
      throw std::invalid_argument("comparison between different SqlValueType");
    if (type == static_cast<SqlValueType>(SqlValueTypeBase::Integer)) {
      return val.Integer < rhs.val.Integer;
    } else if (type == static_cast<SqlValueType>(SqlValueTypeBase::Float)) {
      return val.Float < rhs.val.Float;
    } else {
      return strncmp(val.String, rhs.val.String, Config::kMaxStringLength) < 0;
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
      return strncmp(val.String, rhs.val.String, Config::kMaxStringLength) == 0;
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
  /* the first size_t: the index in Tuple
   * the second size_t: the offset of the attribute in the record */
  map<string, tuple<size_t, SqlValueType, SpecialAttribute, size_t>> attributes;
  map<string, string> indexes;  // attribute name, index name

  /**
   * @brief read raw data from ifstream
   *
   * @param is the ifstream
   */
  void read(ifstream &is);

  /**
   * @brief write raw data into ofstream
   *
   * @param os the ofstream
   */
  void write(ofstream &os) const;
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
