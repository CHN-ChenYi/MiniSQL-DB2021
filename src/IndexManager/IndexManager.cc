#include "IndexManager.hpp"

IndexManager index_manager;

IndexManager::IndexManager(){

}

IndexManager::~IndexManager(){}

bool IndexManager::CreateIndex(const string &table_name, const string &index_name,
                 const string &column){}

bool IndexManager::DeleteIndex(const string &index_name){}

bool IndexManager::InsertKey(const string &index_name,
                 tuple<string, SqlValueType, SpecialAttribute> &attributes,
                 Position &pos){}

bool IndexManager::RemoveKey(const string &index_name,
                    tuple<string, SqlValueType, SpecialAttribute> &attributes){}

vector<Tuple> IndexManager::SelectRecord(const string &index,
                             const vector<Condition> &conditions) {}