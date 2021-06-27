#pragma once

#include <tuple>
#include <vector>

using std::tuple;
using std::vector;

#include "DataStructure.hpp"

/**
 * @brief Create a table
 *
 * @param table_name the name of the table
 * @param attributes the attributes of the table
 * @return true if successful
 */
bool CreateTable(
    const string &table_name,
    const vector<tuple<string, SqlValueType, SpecialAttribute>> &attributes);

/**
 * @brief Drop a table
 *
 * @param table_name the name of the table
 */
bool DropTable(const string &table_name);

/**
 * @brief Create an index on a table
 *
 * @param table_name the name of the table
 * @param index_name the name of the index
 * @param column the column to be created an index on
 */
bool CreateIndex(const string &table_name, const string &index_name,
                 const string &column);

/**
 * @brief Drop an index
 *
 * @param table_name the name of the table
 * @param index_name the name of the index
 */
bool DropIndex(const string &table_name, const string &index_name);

/**
 * @brief Select specified records from a table (Print it to stdout)
 *
 * @param table_name the name of the table
 * @param conditions the specified conditions. If size == 0, select all the
 * records.
 * @param redirect whether dump output to file or not
 */
vector<Tuple> Select(const string &table_name,
                     const vector<Condition> &conditions);

/**
 * @brief Insert a record into a table
 *
 * @param table_name the name of the table
 * @param tuple the record
 */
size_t Insert(const string &table_name, const Tuple &tuple);

size_t InsertFast(const Table &table, const Tuple &tp,
                  const vector<tuple<const char *, size_t, size_t>> &unique);

/**
 * @brief Delete specified records from a table
 *
 * @param table_name the name of the table
 * @param conditions the specified conditions. If size == 0, delete all the
 * records.
 */
size_t Delete(const string &table_name, const vector<Condition> &conditions);
