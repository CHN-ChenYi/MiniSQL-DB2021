#pragma once

#include <unordered_map>
using std::unordered_map;

#include "DataStructure.hpp"

struct CatalogBlock : public Block {};

class CatalogManager {
  unordered_map<string, Table> tables_;
  inline static const string kCatalogFileName = "Catalog.data";

 public:
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
   * @return true if successful
   */
  bool CreateTable(
      const string &table_name,
      const vector<tuple<string, SqlValueType, SpecialAttribute>> &attributes);

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

  void PrintTables() const {
    // TODO
  }
};

extern CatalogManager catalog_manager;
