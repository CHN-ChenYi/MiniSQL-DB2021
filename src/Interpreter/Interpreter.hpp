#pragma once
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "API.hpp"
#include "CatalogManager.hpp"
#include "DataStructure.hpp"

class Interpreter {
 public:
  /**
   * @brief Read commands from stdin, interpret them and then execute them by
   * API module. Return when user quits.
   *
   */
  void interpret();
  /**
   * @brief Read commands from a file, interpret them and then execute them by
   * API module.
   *
   */
  bool interpretFile(const std::filesystem::path &filename);
  /**
   * @brief Set the base directory for relative path
   *
   */
  void setWorkdir(const std::filesystem::path &dir) { cur_dir = dir; }

 private:
  static inline std::unordered_set<std::string_view> keywords = {
      "select", "insert",  "create", "drop",     "delete", "table", "index",
      "from",   "where",   "quit",   "execfile", "unique", "into",  "values",
      "on",     "primary", "key",    "and",      "char",   "int",   "float",
  };

  enum class TokenKind {
    None,
    KeyWord,
    RelOp,
    Int,
    Float,
    Id,
    StrLit,
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
  std::vector<string> select_attributes;
  std::vector<Token> cur_values;
  std::vector<Condition> cur_conditions;
  std::filesystem::path cur_dir = "";

  void tokenToSqlValue(SqlValue &val, const Token &tok);
  SqlValue tokenToSqlValue(const Token &tok);
  Operator tokenToRelOp(const Token &tok);
  void expectEnd();
  void outputUntilNextSpace();
  void skipSpace();
  void expect(std::string_view s);
  bool peek(std::string_view s);
  bool consume(std::string_view s);
  void skip(std::string_view s);
  void parseAttributeList();
  void parseAttribute();
  void parseClauseAttributeList();
  void parseValueList();
  void parseValue();
  void parseConstraint();
  void parseStringLiteral();
  void parseNumber();
  void parseId();
  void parseRelOp();
  void parseCreateTable();
  void parseCreateIndex();
  void parseSelectStat();
  void parseDeleteStat();
  void parseInsertStat();
  void parseDropTable();
  void parseDropIndex();
  void parseExec();
  void parseStatEnd();
  void parseWhereClause();
  void parseBooleanClause();
  void parseRelationClause();
  bool parse();
  bool parseLine();

 public:
  friend std::ostream &operator<<(std::ostream &out, const Token &tok);
};


extern Interpreter interpreter;
