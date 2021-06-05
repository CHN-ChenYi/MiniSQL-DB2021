#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>

#include "API.hpp"

class Interpreter {
 public:
  /**
   * @brief Read commands from stdin, interpret them and then execute them by
   * API module. Return when user quits.
   *
   */
  void Interpret();

 private:
  /*
  static inline constexpr InputType keywords[] = {
      "select", "insert",  "create", "drop",     "delete", "table", "index",
      "from",   "where",   "quit",   "execfile", "unique", "into",  "values",
      "on",     "primary", "key",    "and",      "char",   "int",   "float",
  };

  static inline constexpr InputType cmpOpLiterals[] = {
      "", "<=", ">=", "<>", "<", ">"};
  */
  enum class TokenKind {
    None,
    KeyWord,
    CmpOp,
    ArithOp,
    Space,
    Num,
    Id,
    StrLit,
    Path,
  };

  struct Token {
    TokenKind kind;
    std::string_view sv;
    double f;
    std::int64_t i;
  };

  static inline constexpr Token TokenNone =
      Token{.kind = TokenKind::None, .sv = "", .f = 0.0, .i = 0};

  Token cur_tok, table_name, index_name, indexed_column_name;
  std::string input;
  std::string::iterator iter;
  std::vector<tuple<string, SqlValueType, SpecialAttribute>> cur_attributes;

  void skipSpace();
  void expect(std::string_view s);
  bool peek(std::string_view s);
  bool consume(std::string_view s);
  void skip(std::string_view s);
  void parseAttributeList();
  void parseAttribute();
  void parseConstraint();
  void parseNumber();
  void parseId();
  void parseCreateTable();
  void parseCreateIndex();
  void parseSelectStat();
  void parseDeleteStat();
  void parseInsertStat();
  void parseDropTable();
  void parseDropIndex();
  void parseExec();
  void parseStatEnd();
  bool parse();
};

extern Interpreter interpreter;
