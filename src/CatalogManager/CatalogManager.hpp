#pragma once

#include <unordered_map>
using std::unordered_map;

#include "DataStructure.hpp"

struct CatalogBlock : public Block {};

class CatalogManager {
 public:
   unordered_map<string, Table> tables_;

  /**
   * @brief Construct a new Catalog Manager object. Open the file.
   *
   */
  CatalogManager();

  /**
   * @brief Destroy the Catalog Manager object. Write back to the file.
   *
   */
  ~CatalogManager();

  /**
   * @brief Create a table
   *
   * @param table_name the name of the table
   * @param attributes the attributes of the table
   */
  void CreateTable(
      const string &table_name,
      const vector<tuple<string, SqlValueType, SpecialAttribute>> &attributes);

  /**
   * @brief Create an index
   *
   * @param table_name the name of the table
   * @param attribute_name the name of the attribute
   * @param index_name the name of the index
   * @return true if successful
   */
  void CreateIndex(const string &table_name, const string &attribute_name,
                   const string &index_name);

  /**
   * @brief Drop an index
   *
   * @param table_name the name of the table
   * @param index_name the name of the index
   * @return true if successful
   */
  void DropIndex(const string &table_name, const string &index_name);

  /**
   * @brief Fetch a table's catalog
   *
   * @param table_name the name of the table
   * @return the information of the table
   */
  const Table &TableInfo(const string &table_name);

  /**
   * @brief Drop a table
   *
   * @param table_name the name of the table
   */
  void DropTable(const string &table_name);
};

extern CatalogManager catalog_manager;
