#pragma once
#include "SQLConnector.h"
#include "DataType.h"
#include "Common.h"
#include <memory>
#include <mysql/jdbc.h>

class MySQLConnector : public SQLConnector
{
public:
	MySQLConnector();
	~MySQLConnector();

	bool connect(const std::string &url, const std::string &user, const std::string &pwd,
		const std::string &schema) override;
	void disconnect() override;
	void buildAllTableSchemas(std::vector<SQLNormalTableSchema*>& tableSchemas) override;
	// directColumnName=true indicate column name in sql statement is same as field name in resultTable
	std::vector<SQLRecord *> select(const std::string &sqlStr, MyVariants &params,
		std::vector<int8_t>& types, SQLTable *resultTable,
		bool directColumnName = false) override;
	void select(const std::string &sqlStr, WriteBuffer* buffer, MyVariants &params, 
		std::vector<int8_t>& types) override;

	int update(const std::string &sqlStr, MyVariants &params, 
		std::vector<int8_t>& types) override;
	int64_t insert(const std::string &sqlStr, MyVariants& params,
		std::vector<int8_t> &types) override;

	void startTransaction() override;
	void commit() override;
	void rollBack() override;

private:
	void readSqlResult(sql::ResultSet *res, std::unordered_map<std::string, MyVariant>& values);
	void writeResultSchema(sql::ResultSetMetaData *metaData, WriteBuffer* buffer,
		std::vector<std::string> &columnNames, std::vector<DataType> &dataTypes);
	void writeRecord(std::unordered_map<std::string, MyVariant>& sqlResult, WriteBuffer* buffer,
		const std::vector<std::string> &columnNames, const std::vector<DataType> &dataTypes);
	DataType sqlTypeToDataType(int type);
	DataType sqlTypeToDataType(const std::string &typeStr);
	int dataTypeToSqlType(ParamDataType value);
	void setParam(sql::PreparedStatement *stmt, int index, const MyVariant& param, uint8_t type);

private:
	sql::Connection *m_con;
};
