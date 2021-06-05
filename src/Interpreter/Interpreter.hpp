#pragma once
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>

class Interpreter {
 public:
  using InputType = std::string_view;
  /**
   * @brief Read commands from stdin, interpret them and then execute them by
   * API module. Return when user quits.
   *
   */
  void Interpret() const;

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
    INum,
    RNum,
    Id,
    StrLit,
    Path,
  };

  struct Token {
    TokenKind kind;
    union {
      double f;
      int i;
      InputType sv;
    };
  };

  static inline constexpr Token TokenNone = Token{.kind = TokenKind::None, .sv = ""};

  using ParserState = std::tuple<bool, Token, InputType>;

  void handleError(std::string_view msg, InputType pos) const;
  ParserState skipSpace(InputType input) const;
  ParserState expect(std::string_view lit, InputType input) const;
  ParserState peek(std::string_view lit, InputType input) const;
  ParserState parseNumber(InputType input) const;
  ParserState parseId(InputType input) const;
  ParserState parsePath(InputType input) const;
  ParserState parseCreateTable(InputType input) const;
  ParserState parseCreateIndex(InputType input) const;
  ParserState parseSelectStat(InputType input) const;
  ParserState parseDeleteStat(InputType input) const;
  ParserState parseInsertStat(InputType input) const;
  ParserState parseDropTable(InputType input) const;
  ParserState parseDropIndex(InputType input) const;
  ParserState parseExec(InputType input) const;
};

extern Interpreter interpreter;
