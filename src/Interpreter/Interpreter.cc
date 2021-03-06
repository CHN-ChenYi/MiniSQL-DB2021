#include "Interpreter.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "API.hpp"
#include "CatalogManager.hpp"
#include "DataStructure.hpp"

using std::cerr;
using std::cin;
using std::cout;
using std::endl;
using std::int64_t;
using std::isalnum;
using std::isalpha;
using std::isspace;
using std::max;
using std::ostream;
using std::strchr;
using std::string;
using std::string_view;
using std::stringstream;
using std::strtod;
using std::unordered_map;
using std::chrono::steady_clock;
using std::filesystem::path;

using namespace std::literals;

int icasecmp(const char *s1, const char *s2, size_t n) {
  return strncasecmp(s1, s2, n);
}

void Interpreter::showAffected() { showAffected(affected); }

void Interpreter::showAffected(size_t cnt) {
  std::cout << ANSI_COLOR_CYAN << cnt
            << ANSI_COLOR_RESET " rows in set / affect" << std::endl;
}

void Interpreter::interpret() {
  extern volatile std::sig_atomic_t interrupt;
  for (;;) {
    cur_tok = table_name = index_name = indexed_column_name = TokenNone;
    cur_attributes.clear();
    cur_values.clear();
    select_attributes.clear();
    cur_conditions.clear();

    cout << ANSI_COLOR_BLUE "MiniSQL > " ANSI_COLOR_RESET;
    if (!getline(cin, input)) {
      cout << endl;
      break;
    }

    auto t1 = steady_clock::now();

    iter = input.begin();
    bool need_quit = false;
    cleanAffected();
    try {
      need_quit = parseLine();
    } catch (const syntax_error &err) {
      cerr << ANSI_COLOR_RED "syntax error: " ANSI_COLOR_RESET << err.what()
           << endl;
    } catch (const invalid_ident &err) {
      cerr << ANSI_COLOR_RED "invalid identifier: " ANSI_COLOR_RESET
           << err.what() << endl;
    } catch (const invalid_value &err) {
      cerr << ANSI_COLOR_RED "invalid value: " ANSI_COLOR_RESET << err.what()
           << endl;
    } catch (const invalid_index_attribute &err) {
      cerr << ANSI_COLOR_RED "invalid attribute for indexing: " ANSI_COLOR_RESET
           << err.what() << endl;
    }
    showAffected();
    if (need_quit) break;
    if (interrupt) {
      cout << endl;
      break;
    }

    auto t2 = steady_clock::now();
    std::chrono::duration<double> diff = t2 - t1;
    cout << "run command above in " ANSI_COLOR_MAGENTA << diff.count()
         << "s" ANSI_COLOR_RESET << endl;
  }
}

tuple<bool, size_t> Interpreter::interpretFile(const path &filename) {
  extern volatile std::sig_atomic_t interrupt;
  auto t1 = steady_clock::now();
  ifstream is(cur_dir / filename, std::ios::binary);
  stringstream buf;
  size_t sentence_cnt = 0;
  if (!is) {
    cerr << "can't open file " << cur_dir / filename << endl;
    return {false, 0};
  }
  buf << is.rdbuf();
  input = buf.str();
  iter = input.begin();
  skipSpace();

  cleanAffected();
  for (; iter != input.end(); ++sentence_cnt) {
    cur_tok = table_name = index_name = indexed_column_name = TokenNone;
    cur_attributes.clear();
    cur_values.clear();
    select_attributes.clear();
    cur_conditions.clear();

    bool need_quit = false;
    try {
      need_quit = parse();
    } catch (const syntax_error &err) {
      cerr << ANSI_COLOR_RED "syntax error: " ANSI_COLOR_RESET << err.what()
           << endl;
      break;
    } catch (const invalid_ident &err) {
      cerr << ANSI_COLOR_RED "invalid identifier: " ANSI_COLOR_RESET
           << err.what() << endl;
      break;
    } catch (const invalid_value &err) {
      cerr << ANSI_COLOR_RED "invalid value: " ANSI_COLOR_RESET << err.what()
           << endl;
      break;
    } catch (const invalid_index_attribute &err) {
      cerr << ANSI_COLOR_RED "invalid attribute for indexing: " ANSI_COLOR_RESET
           << err.what() << endl;
      break;
    }
    if (need_quit || interrupt) break;
    skipSpace();
  }

  auto t2 = steady_clock::now();
  std::chrono::duration<double> diff = t2 - t1;
  cout << "run " ANSI_COLOR_GREEN << sentence_cnt
       << ANSI_COLOR_RESET " sentences in " ANSI_COLOR_MAGENTA << diff.count()
       << "s" ANSI_COLOR_RESET << endl;
  return {true, affected};
}

void Interpreter::checkAndFixCondition() {
  auto name = string(table_name.sv);
  auto &table = catalog_manager.TableInfo(name);
  for (auto &cond : cur_conditions) {
    if (!table.attributes.contains(cond.attribute)) {
      std::cerr << "no such an attribute `" ANSI_COLOR_RED << cond.attribute
                << ANSI_COLOR_RESET " `referenced in condition" << std::endl;
      throw invalid_ident("invalid attribute name");
    }
    auto &[_1, type, _2, _3] = table.attributes.find(cond.attribute)->second;
    switch (type) {
      case static_cast<SqlValueType>(SqlValueTypeBase::Integer):
        if (cond.val.type >=
            static_cast<SqlValueType>(SqlValueTypeBase::String)) {
          throw syntax_error("incompatible condition");
        }
        if (cond.val.type ==
            static_cast<SqlValueType>(SqlValueTypeBase::Float)) {
          cond.val.type = static_cast<SqlValueType>(type);
          float tmp = cond.val.val.Float;
          cond.val.val.Integer = tmp;
        }
        break;
      case static_cast<SqlValueType>(SqlValueTypeBase::Float):
        if (cond.val.type >=
            static_cast<SqlValueType>(SqlValueTypeBase::String)) {
          throw syntax_error("incompatible condition");
        }
        if (cond.val.type ==
            static_cast<SqlValueType>(SqlValueTypeBase::Integer)) {
          cond.val.type = static_cast<SqlValueType>(type);
          int tmp = cond.val.val.Integer;
          cond.val.val.Float = tmp;
        }
        break;
      default:
        if (cond.val.type <
            static_cast<SqlValueType>(SqlValueTypeBase::String)) {
          throw syntax_error("incompatible condition");
        }
        if (cond.val.type > static_cast<SqlValueType>(type)) {
          cerr << "warning: string is too long" << endl;
        }
        cond.val.type = static_cast<SqlValueType>(type);
        break;
    }
  }
}

void Interpreter::expectEnd() {
  skipSpace();
  if (iter != input.end()) {
    cerr << "warning: extra token `";
    outputUntilNextSpace();
    cerr << "`" << endl;
  }
}

void Interpreter::outputUntilNextSpace() {
  auto p = iter;
  if (p == input.end())
    cerr << "EOF";
  else
    while (p != input.end() && !strchr("\t\n\r ", *p)) cerr << *p++;
}

ostream &operator<<(ostream &out, const Interpreter::Token &tok) {
  switch (tok.kind) {
    case Interpreter::TokenKind::Int:
      out << "int: " << tok.i;
      break;
    case Interpreter::TokenKind::Float:
      out << "float: " << tok.f;
      break;
    case Interpreter::TokenKind::StrLit:
      out << "str: " << tok.sv;
      break;
    default:
      assert(0);
  }
  return out;
}

void Interpreter::skipSpace() {
  for (; iter != input.end() && isspace(*iter); ++iter)
    ;
}

void Interpreter::expect(string_view s) {
  skipSpace();
  auto diff = input.end() - iter;
  auto len = static_cast<string::difference_type>(s.length());
  if (diff >= len && icasecmp(&*iter, s.data(), s.size()) == 0)
    if (!keywords.contains(s) || diff == len || !isalnum(iter[s.size()])) {
      iter += s.size();
      return;
    }
  cerr << "expect token `" ANSI_COLOR_GREEN << s
       << ANSI_COLOR_RESET "` but got `" ANSI_COLOR_RED;
  outputUntilNextSpace();
  cerr << ANSI_COLOR_RESET "`" << endl;
  throw syntax_error("can't find expected token");
}

bool Interpreter::consume(string_view s) {
  skipSpace();
  auto diff = input.end() - iter;
  auto len = static_cast<string::difference_type>(s.length());
  if (diff >= len && icasecmp(&*iter, s.data(), s.size()) == 0)
    if (!keywords.contains(s) || diff == len || !isalnum(iter[s.size()])) {
      iter += s.size();
      return true;
    }
  return false;
}

bool Interpreter::peek(string_view s) {
  skipSpace();
  auto diff = input.end() - iter;
  auto len = static_cast<string::difference_type>(s.length());
  if (diff >= len && icasecmp(&*iter, s.data(), s.size()) == 0)
    if (!keywords.contains(s) || diff == len || !isalnum(iter[s.size()])) {
      return true;
    }
  return false;
}

void Interpreter::skip(string_view s) { iter += s.size(); }

void Interpreter::parseAttribute() {
  Token attr_tok;
  SqlValueType val_type;
  SpecialAttribute spec_attr;

  skipSpace();
  parseId();
  attr_tok = cur_tok;

  if (peek("integer")) {
    skip("integer");
    val_type = static_cast<SqlValueType>(SqlValueTypeBase::Integer);
  } else if (peek("int")) {
    skip("int");
    val_type = static_cast<SqlValueType>(SqlValueTypeBase::Integer);
  } else if (peek("float")) {
    skip("float");
    val_type = static_cast<SqlValueType>(SqlValueTypeBase::Float);
  } else if (peek("char")) {
    skip("char");
    expect("(");
    parseNumber();
    if (cur_tok.kind != TokenKind::Int ||
        (cur_tok.i < 0 || cur_tok.i >= Config::kMaxStringLength))
      throw syntax_error("invalid length of char");
    size_t n = cur_tok.i / 16; /* padding size to 16 */
    size_t r = cur_tok.i % 16;
    val_type = static_cast<SqlValueType>(SqlValueTypeBase::String) +
               (unsigned)(n + !!r) * 16;
    expect(")");
  } else {
    cerr << "expect a valid attribute type among " ANSI_COLOR_GREEN
            "[int, float, char]" ANSI_COLOR_RESET
         << endl;
    throw syntax_error("invalid attribute type");
  }

  if (peek("unique")) {
    skip("unique");
    spec_attr = SpecialAttribute::UniqueKey;
  } else {
    spec_attr = SpecialAttribute::None;
  }

  cur_attributes.push_back({string(attr_tok.sv), val_type, spec_attr});
}

void Interpreter::parseConstraint() {
  skipSpace();
  expect("primary");
  expect("key");
  expect("(");
  parseId();
  bool found = false;
  for (auto &[name, type, spec] : cur_attributes) {
    if (name == cur_tok.sv) {
      found = true;
      spec = SpecialAttribute::PrimaryKey;
    }
  }
  if (!found) {
    cerr << "no such attribute named `" << cur_tok.sv << "`" << endl;
    throw syntax_error("constraint failed");
  }
  expect(")");
}

void Interpreter::parseAttributeList() {
  skipSpace();
  parseAttribute();
  while (consume(",")) {
    if (peek("primary")) {
      parseConstraint();
    } else {
      parseAttribute();
    }
  }
}

void Interpreter::parseClauseAttributeList() {
  skipSpace();
  parseId();
  select_attributes.emplace_back(cur_tok.sv);
  while (consume(",")) {
    parseId();
    select_attributes.emplace_back(cur_tok.sv);
  }
}

void Interpreter::parseNumber() {
  skipSpace();
  if (iter != input.end()) {
    char *end1 = nullptr, *end2 = nullptr, *end;

    auto val_f = strtod(&*iter, &end1);
    auto val_i = strtoll(&*iter, &end2, 10);

    end = max(end1, end2);

    if (end != &*iter) {
      iter += end - &*iter;
      cur_tok = Token{
          .kind = end == end2 ? TokenKind::Int : TokenKind ::Float,
          .sv = ""sv,
          .f = val_f,
          .i = val_i,
      };
      return;
    }
  }
  cerr << "expect a number but got `" ANSI_COLOR_RED;
  outputUntilNextSpace();
  cerr << ANSI_COLOR_RESET "`" << endl;
  throw syntax_error("can't find expected token");
}

void Interpreter::parseStringLiteral() {
  skipSpace();
  decltype(iter) begin, end;
  if (peek("`"sv)) {
    skip("`"sv);
    begin = iter;
    while (iter != input.end() && *iter != '`') ++iter;
    end = iter;
    expect("`"sv);
  } else if (peek("'"sv)) {
    skip("'"sv);
    begin = iter;
    while (iter != input.end() && *iter != '\'') ++iter;
    end = iter;
    expect("'"sv);
  } else if (peek("\""sv)) {
    skip("\""sv);
    begin = iter;
    while (iter != input.end() && *iter != '"') ++iter;
    end = iter;
    expect("\""sv);
  } else {
    cerr << "expect valid string literal quoted by ` ' or \" but got "
            "`" ANSI_COLOR_RED;
    outputUntilNextSpace();
    cerr << ANSI_COLOR_RESET "`" << endl;
    throw syntax_error("can't find expected token");
  }
  cur_tok = Token{
      .kind = TokenKind::StrLit, .sv = string_view(begin, end), .f = 0, .i = 0};
  if (cur_tok.sv.length() == 0 ||
      cur_tok.sv.length() >= Config::kMaxStringLength) {
    cerr << "expect valid string literal of which length is [1, 256), and we "
            "got " ANSI_COLOR_RED
         << cur_tok.sv.length() << ANSI_COLOR_RESET << endl;
    throw syntax_error("invalid string literal length");
  }
}

void Interpreter::parseId() {
  skipSpace();
  if (iter != input.end()) {
    if (isalpha(iter[0])) {
      auto start = iter;
      for (; iter != input.end() && isalnum(*iter); ++iter)
        ;
      auto tok = string_view(start, iter);
      cur_tok = Token{.kind = TokenKind::Id, .sv = tok, .f = 0.0, .i = 0};
      return;
    }
  }
  cerr << "expect valid identifier like `" ANSI_COLOR_GREEN
          "[A-Za-z][A-Za-z0-9]*" ANSI_COLOR_RESET "` but got `" ANSI_COLOR_RED;
  outputUntilNextSpace();
  cerr << ANSI_COLOR_RESET "`" << endl;
  throw syntax_error("can't find expected token");
}

void Interpreter::parseRelOp() {
  skipSpace();
  static string_view ops[] = {
      ">=", "<=", "<>", ">", "<", "=",
  };
  int idx = -1;
  for (size_t i = 0; i < sizeof(ops) / sizeof(*ops); ++i) {
    if (peek(ops[i])) {
      skip(ops[i]);
      idx = i;
      break;
    }
  }
  if (idx == -1) {
    cerr << "expect relation operator like " ANSI_COLOR_GREEN
            "< > <= >= <> =" ANSI_COLOR_RESET " but got `" ANSI_COLOR_RED;
    outputUntilNextSpace();
    cerr << ANSI_COLOR_RESET "`" << endl;
    throw syntax_error("can't find expected token");
  }
  cur_tok = Token{.kind = TokenKind::RelOp, .sv = ops[idx], .f = 0, .i = 0};
}

void Interpreter::parseCreateTable() {
  expect("create"sv);
  expect("table"sv);
  parseId();
  table_name = cur_tok;
  expect("("sv);
  parseAttributeList();
  expect(")"sv);
  parseStatEnd();
#ifdef _INTERPRETER_DEBUG
  cout << "DEBUG: create a table named `" << table_name.sv << "`" << endl;
#endif

  CreateTable(string(table_name.sv), cur_attributes);
}

void Interpreter::parseCreateIndex() {
  expect("create"sv);
  expect("index"sv);
  parseId();
  index_name = cur_tok;
  expect("on");
  parseId();
  table_name = cur_tok;
  expect("("sv);
  parseId();
  indexed_column_name = cur_tok;
  expect(")"sv);
  parseStatEnd();
#ifdef _INTERPRETER_DEBUG
  cout << "DEBUG: create an index on `" << table_name.sv << "."
       << indexed_column_name.sv << "` named `" << index_name.sv << "`" << endl;
#endif

  CreateIndex(string(table_name.sv), string(index_name.sv),  string(indexed_column_name.sv));
}

void Interpreter::parseDropTable() {
  expect("drop"sv);
  expect("table"sv);
  parseId();
  table_name = cur_tok;
  parseStatEnd();
#ifdef _INTERPRETER_DEBUG
  cout << "DEBUG: drop a table named `" << table_name.sv << "`" << endl;
#endif

  last_insert_table_name.clear();
  DropTable(string(table_name.sv));
}

void Interpreter::parseDropIndex() {
  expect("drop"sv);
  expect("index"sv);
  parseId();
  index_name = cur_tok;
  expect("on"sv);
  parseId();
  table_name = cur_tok;
  parseStatEnd();
#ifdef _INTERPRETER_DEBUG
  cout << "DEBUG: drop a index named `" << index_name.sv << "`" << endl;
#endif

  DropIndex(string(table_name.sv), string(index_name.sv));
}

void Interpreter::parseExec() {
  expect("execfile");
  skipSpace();

  auto line_end_pos = input.find_first_of("\r\n", iter - input.begin());
  auto path_end_pos = input.find_last_of(";.", line_end_pos);
  if (path_end_pos == input.npos) {
    cerr << "not correct format of execfile" << endl;
    throw syntax_error("wrong execfile sentence");
  }
  auto len = path_end_pos - (iter - input.begin());
  path file_path(string(&*iter, len));

  iter += len;
  parseStatEnd();

#ifdef _INTERPRETER_DEBUG
  cout << "DEBUG: exec " << file_path << endl;
#endif
  Interpreter child_interpreter;
  if (file_path.is_absolute()) {
    child_interpreter.setWorkdir(file_path.parent_path());
  } else if (file_path.is_relative()) {
    child_interpreter.setWorkdir(cur_dir / file_path.parent_path());
  }
  auto [_, count] = child_interpreter.interpretFile(file_path.filename());
  addAffected(count);
}

void Interpreter::parseValueList() {
  skipSpace();
  parseValue();
  while (consume(",")) {
    parseValue();
  }
}

void Interpreter::parseValue() {
  if (peek("'"sv) || peek("\""sv) || peek("`"sv))
    parseStringLiteral();
  else
    parseNumber();
  cur_values.push_back(cur_tok);
}

void Interpreter::parseSelectStat() {
  bool redirect = false;
  expect("select"sv);
  if (peek("*"sv)) {
    skip("*"sv);
  } else {
    parseClauseAttributeList();
  }
  expect("from"sv);
  parseId();
  table_name = cur_tok;
  if (peek("where"sv)) parseWhereClause();
  if (consume("#"sv)) redirect = true;
  parseStatEnd();
#ifdef _INTERPRETER_DEBUG
  cout << "DEBUG: select";
  if (select_attributes.empty())
    cout << " *" << endl;
  else {
    cout << endl;
    for (auto &attr : select_attributes) cout << "  " << attr << ", " << endl;
  }
  cout << "from `" << table_name.sv << "` with condition";
  if (cur_conditions.empty())
    cout << " ~" << endl;
  else {
    cout << endl;
    for (auto &v : cur_conditions) {
      cout << "  " << v.attribute << " " << (unsigned)v.op << " "
           << (unsigned)v.val.type << endl;
    }
  }
#endif

  checkAndFixCondition();

  auto res = Select(string(table_name.sv), cur_conditions);
  std::ofstream out("output.txt");
  if (redirect) {
    out << "+" << string(32, '-') << "+" << std::endl;
    for (auto &v : res) {
      out << static_cast<std::string>(v) << std::endl;
    }
  } else {
    std::cout << "+" << string(32, '-') << "+" << std::endl;
    for (auto &v : res) {
      std::cout << static_cast<std::string>(v) << std::endl;
    }
  }
  addAffected(res.size());
}

void Interpreter::parseDeleteStat() {
  expect("delete"sv);
  if (!peek("from")) {
    parseClauseAttributeList();
  }
  expect("from"sv);
  parseId();
  table_name = cur_tok;
  if (peek("where")) parseWhereClause();
  parseStatEnd();

#ifdef _INTERPRETER_DEBUG
  cout << "DEBUG: delete";
  if (select_attributes.empty())
    cout << " *" << endl;
  else {
    cout << endl;
    for (auto &attr : select_attributes) cout << "  " << attr << ", " << endl;
  }
  cout << "from `" << table_name.sv << "` with condition";
  if (cur_conditions.empty())
    cout << " ~" << endl;
  else {
    cout << endl;
    for (auto &v : cur_conditions) {
      cout << "  " << v.attribute << " " << (unsigned)v.op << " "
           << (unsigned)v.val.type << endl;
    }
  }
#endif
  checkAndFixCondition();
  addAffected(Delete(string(table_name.sv), cur_conditions));
}

void Interpreter::parseInsertStat() {
  bool not_changed;
  expect("insert"sv);
  expect("into"sv);
  parseId();
  if (cur_tok.sv == last_insert_table_name)
    not_changed = true;
  else {
    not_changed = false;
    last_insert_table_name.assign(cur_tok.sv);
  }
  table_name = cur_tok;
  expect("values"sv);
  expect("("sv);
  parseValueList();
  expect(")"sv);
  parseStatEnd();
  if (cur_values.size() > 31) {
    cerr << "WARNING: the count of values is large than 31" << endl;
  }
#ifdef _INTERPRETER_DEBUG
  cout << "DEBUG: insert into `" << table_name.sv << "` with values" << endl;
  for (auto &v : cur_values) {
    cout << "  " << v << endl;
  }
#endif

  if (!not_changed) {
    last_table = &catalog_manager.TableInfo(string(table_name.sv));
    tp = last_table->makeEmptyTuple();
  }
  if (tp.values.size() != cur_values.size()) {
    cerr << "the number of values doesn't match" << endl;
    throw syntax_error("the number of value wrong");
  }
  for (size_t i = 0; i < tp.values.size(); ++i) {
    tokenToSqlValue(tp.values[i], cur_values[i]);
  }
  if (!not_changed) {
    need_unique.clear();
    for (auto &[_1, value] : last_table->attributes) {
      auto &[idx, type, special, offset] = value;
      if (special >= SpecialAttribute::PrimaryKey) {
        size_t len = type;
        if (type >= static_cast<SqlValueType>(SqlValueTypeBase::String))
          len -= static_cast<SqlValueType>(SqlValueTypeBase::String);
        else
          len = sizeof(int);
        need_unique.push_back(
            {reinterpret_cast<const char *>(&tp.values[idx].val), len, offset});
      }
    }
  }
  addAffected(InsertFast(*last_table, tp, need_unique));
}

void Interpreter::parseWhereClause() {
  expect("where"sv);
  parseBooleanClause();
}

void Interpreter::tokenToSqlValue(SqlValue &val, const Token &tok) {
  switch (tok.kind) {
    case Interpreter::TokenKind::Int:
      if (val.type >= static_cast<SqlValueType>(SqlValueTypeBase::String)) {
        cerr << "the types of the table and values don't match" << endl;
        throw invalid_value("type error");
      }
      if (val.type == static_cast<SqlValueType>(SqlValueTypeBase::Integer))
        val.val.Integer = tok.i;
      else
        val.val.Float = tok.i;
      break;
    case Interpreter::TokenKind::Float:
      if (val.type >= static_cast<SqlValueType>(SqlValueTypeBase::String)) {
        cerr << "the types of the table and values don't match" << endl;
        throw invalid_value("type error");
      }
      if (val.type == static_cast<SqlValueType>(SqlValueTypeBase::Integer))
        val.val.Integer = tok.f;
      else
        val.val.Float = tok.f;
      break;
    case Interpreter::TokenKind::StrLit:
      if (val.type < tok.sv.length() +
                         static_cast<SqlValueType>(SqlValueTypeBase::String)) {
        cerr << "the types of the table and values don't match: requires a "
                "number"
             << endl;
        throw invalid_value("type error");
      }
      memcpy(val.val.String, tok.sv.data(), tok.sv.size());
      if (tok.sv.size() < Config::kMaxStringLength)
        memset(val.val.String + tok.sv.size(), 0,
               Config::kMaxStringLength - tok.sv.size());
      break;
    default:
      assert(0);
  }
}

SqlValue Interpreter::tokenToSqlValue(const Token &tok) {
  SqlValue val;
  switch (tok.kind) {
    case Interpreter::TokenKind::Int:
      val.type = static_cast<SqlValueType>(SqlValueTypeBase::Integer);
      val.val.Integer = tok.i;
      break;
    case Interpreter::TokenKind::Float:
      val.type = static_cast<SqlValueType>(SqlValueTypeBase::Float);
      val.val.Float = tok.f;
      break;
    case Interpreter::TokenKind::StrLit:
      val.type =
          static_cast<SqlValueType>(SqlValueTypeBase::String) + tok.sv.length();
      memcpy(val.val.String, tok.sv.data(), tok.sv.size());
      if (tok.sv.size() < Config::kMaxStringLength)
        memset(val.val.String + tok.sv.size(), 0,
               Config::kMaxStringLength - tok.sv.size());
      break;
    default:
      assert(0);
  }
  return val;
}

Operator Interpreter::tokenToRelOp(const Token &tok) {
  assert(tok.kind == TokenKind::RelOp);
  static unordered_map<string_view, Operator> tok2op = {
      {"<", Operator::LT},  {">", Operator::GT}, {">=", Operator::GE},
      {"<=", Operator::LE}, {"=", Operator::EQ}, {"<>", Operator::NE},
  };
  if (!tok2op.contains(tok.sv)) {
    throw syntax_error("not correct op");
  }
  return tok2op[tok.sv];
}

void Interpreter::parseRelationClause() {
  Token attr_name;
  Token op;
  Token value;
  parseId();
  attr_name = cur_tok;
  parseRelOp();
  op = cur_tok;
  parseValue();
  value = cur_tok;

  cur_conditions.push_back(Condition{.attribute = string(attr_name.sv),
                                     .op = tokenToRelOp(op),
                                     .val = tokenToSqlValue(value)});
}

void Interpreter::parseBooleanClause() {
  parseRelationClause();
  for (;;) {
    if (consume("and"sv)) {
      parseRelationClause();
      continue;
    }
    break;
  }
}

void Interpreter::parseStatEnd() {
  if (!consume(";"sv)) expect("."sv);
}

void Interpreter::parseComment() {
  expect("--");
  while (iter != input.end() && !strchr("\t\n\r", *iter)) iter++;
}

bool Interpreter::parse() {
  auto backup = iter;
  if (peek("insert")) {
    parseInsertStat();
  } else if (consume("create")) {
    if (peek("table")) {
      iter = backup;
      parseCreateTable();
    } else if (peek("index")) {
      iter = backup;
      parseCreateIndex();
    } else {
      expect("table");
    }
  } else if (consume("drop")) {
    if (peek("table")) {
      iter = backup;
      parseDropTable();
    } else if (peek("index")) {
      iter = backup;
      parseDropIndex();
    } else {
      expect("table");
    }
  } else if (peek("execfile")) {
    parseExec();
  } else if (peek("quit")) {
    skip("quit");
    parseStatEnd();
    expectEnd();
    return true;
  } else if (peek("select")) {
    parseSelectStat();
  } else if (peek("delete")) {
    parseDeleteStat();
  } else if (peek("--")) {
    parseComment();
  } else {
    cerr << "expect a valid sentence" << endl;
    iter = input.end();
  }
  return false;
}

bool Interpreter::parseLine() {
  if (parse()) return true;
  expectEnd();
  return false;
}

Interpreter interpreter;
