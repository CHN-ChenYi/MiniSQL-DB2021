#pragma once

#include <fstream>
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
const int kMaxBlockNum = 128;
}  // namespace Config

enum struct SqlValueTypeBase { Integer, Float, String };
typedef unsigned SqlValueType;  // if >= SqlValueTypeBase::String, it means
                                // char(val - SqlValueTypeBase::String)

struct SqlValue {
  SqlValueType type;
  union {
    int Integer;
    float Float;
    string String;
  } val;
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

class Block {
  bool dirty_;
  char val_[Config::kBlockSize];

 public:
  Block() = default;
  virtual ~Block() = default;
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
  void write(ofstream &os) { os.write(val_, Config::kBlockSize); }
};

struct Position {
  size_t block_id, offset;
};

enum struct Operator { GT, GE, LT, LE, EQ, NE };

struct Condition {
  string attribute;
  Operator op;
  SqlValue val;
};
