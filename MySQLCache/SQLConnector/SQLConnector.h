#pragma once

#include "WriteBuffer.h"
#include <MyVariant.h>
#include <vector>
#include <string>
#include <unordered_map>

class SQLTable;
class SQLRecord;
class SQLNormalTableSchema;

class SQLConnector
{
public:
	virtual bool connect(const std::string &url, const std::string &user, const std::string &pwd,
		const std::string &schema) = 0;
	virtual void disconnect() = 0;
	virtual void buildAllTableSchemas(std::vector<SQLNormalTableSchema*>& tableSchemas) = 0;
	// directColumnName indicate : sql'column name is same as resultTable
	virtual std::vector<SQLRecord *> select(const std::string &sqlStr, 
		MyVariants &params, std::vector<int8_t>& types, SQLTable *resultTable,
		bool directColumnName = false) = 0;
	virtual void select(const std::string &sqlStr, WriteBuffer *buffer, 
		MyVariants &params, std::vector<int8_t>& types) = 0;

	virtual int update(const std::string &sqlStr, MyVariants &params, 
		std::vector<int8_t>& types) = 0;
	virtual int64_t insert(const std::string &sqlStr, MyVariants &params, 
		std::vector<int8_t>& types) = 0;

	virtual void startTransaction() = 0;
	virtual void commit() = 0;
	virtual void rollBack() = 0;
};
