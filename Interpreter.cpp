#include <optional>
#include <string_view>
#include <unordered_set>
#include <utility>


using namespace std;

enum {
  Select,
  Insert,
  Create,
  Drop,
  Delete,
  Table,
  Index,
  From,
  Where,
  Quit,
  Execfile,
  Unique,
  Into,
  Values,
  On,
  Primary,
  Key,
  And,
  Char,
  Int,
  Float,
  CmpOp, // <= >= <>
  ArithOp,
  Space,
  INum,
  RNum,
  StrLit,
  FileName,
};

enum {
  Cmp_None,
  Cmp_LEq,
  Cmp_GEq,
  Cmp_NEq,
  Cmp_Lt,
  Cmp_Gt,
};

// Keywords
constexpr string_view keywords[] = {
    "select", "insert",  "create", "drop",     "delete", "table", "index",
    "from",   "where",   "quit",   "execfile", "unique", "into",  "values",
    "on",     "primary", "key",    "and",      "char",   "int",   "float",
};
constexpr string_view cmpOpLiterals[] = {"", "<=", ">=", "<>", "<", ">"};

// Forward declarations
struct ParserResult;
struct ParserState;

// The rest things for next step
struct ParserRest {
  string_view input;
  unsigned line;
};

// Common token class
struct Token {
  int cmpOpType, arithOpType;
  int keywordType;
  unsigned line;
  double f;
  int i;
  string str;
};

// The parser result
struct ParserResult {
  Token token;
  bool success;
};

// The parser state
struct ParserState {
  ParserResult result;
  ParserRest rest;
};

bool parse(string_view input) { return false; }