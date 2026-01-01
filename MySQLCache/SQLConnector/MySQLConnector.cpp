#include "MySQLConnector.h"
#include "SQLTable.h"
#include "SQLConnectorException.h"
#include <iostream>
#include <sstream>
#include <string>

using namespace std;

MySQLConnector::MySQLConnector() :
	m_con(nullptr)
{
}

MySQLConnector::~MySQLConnector()
{
}

bool MySQLConnector::connect(const std::string &url, const std::string &user, const std::string &pwd,
	const std::string &schema)
{
	sql::mysql::MySQL_Driver *driver = sql::mysql::get_driver_instance();
	m_con = driver->connect(url, user, pwd);
	if (m_con) {
		m_con->setSchema(schema);
		return true;
	}	

	return false;
}

void MySQLConnector::disconnect()
{
	if (m_con) {
		delete m_con;
		m_con = nullptr;
	}
}

void MySQLConnector::buildAllTableSchemas(std::vector<SQLNormalTableSchema*>& tableSchemas)
{
	try {
		std::unique_ptr<sql::Statement> stmt(m_con->createStatement());
		std::unique_ptr <sql::ResultSet> res(
			stmt->executeQuery(string("SELECT table_name, column_name, data_type, column_key FROM information_schema.columns WHERE table_schema = '")
				.append(m_con->getSchema()).append("'")));
		SQLNormalTableSchema* curTableSchema = nullptr;
		string curTableName;
		while (res->next()) {
			string tableName = res->getString("table_name");
			string columnName = res->getString("column_name");
			string dataType = res->getString("data_type");
			string columnKey = res->getString("column_key");
			if (tableName != curTableName) {
				curTableSchema = new SQLNormalTableSchema();
				curTableName = tableName;
				curTableSchema->setName(curTableName);
				tableSchemas.push_back(curTableSchema);
			}

			auto field = curTableSchema->addField(columnName, sqlTypeToDataType(dataType));
			if (columnKey == "PRI") {
				field->setPrimaryKey(true);
			}
		}
	}
	catch (sql::SQLException& e) {
		cerr << "buildAllTableSchemas Error: " << e.what() << endl;
	}
}

std::vector<SQLRecord *> MySQLConnector::select(const std::string &sqlStr, 
	MyVariants &params, std::vector<int8_t>& types, SQLTable *resultTable, bool directColumnName)
{
	vector<SQLRecord *> newRecords;
	try {
		std::unique_ptr<sql::PreparedStatement> stmt(m_con->prepareStatement(sqlStr));
		for (int i = 0; i < params.count(); ++i) {
			setParam(stmt.get(), i + 1, params.variant(i), types[i]);
		}
		std::unique_ptr <sql::ResultSet> res(stmt->executeQuery());
		unordered_map<std::string, MyVariant> values;
		while (res->next()) {
			readSqlResult(res.get(), values);
			SQLRecord *newRec = resultTable->newRecord();
			newRec->read(values, directColumnName);
			SQLRecord *realNewRec = resultTable->append(newRec);
			if (realNewRec != newRec) {
				delete newRec;
			}

			newRecords.push_back(realNewRec);
		}
	}
	catch (sql::SQLException &e) {
		cerr << "Select Error: " << sqlStr << ":" << e.what() << endl;
	}

	return newRecords;
}

void MySQLConnector::select(const std::string &sqlStr, WriteBuffer *buffer, 
	MyVariants &params, std::vector<int8_t>& types)
{
	try {
		std::unique_ptr<sql::PreparedStatement> stmt(m_con->prepareStatement(sqlStr));
		for (int i = 0; i < params.count(); ++i) {
			setParam(stmt.get(), i + 1, params.variant(i), types[i]);
		}
		std::unique_ptr <sql::ResultSet> res(stmt->executeQuery());
		auto metaData = res->getMetaData();
		std::vector<std::string> columnNames;
		std::vector<DataType> dataTypes;
		writeResultSchema(metaData, buffer, columnNames, dataTypes);
		unordered_map<std::string, MyVariant> values;
		while (res->next()) {
			readSqlResult(res.get(), values);
			writeRecord(values, buffer, columnNames, dataTypes);
		}
	}
	catch (sql::SQLException &e) {
		cerr << "Select Error: " << sqlStr << ":" << e.what() << endl;
	}
}

int MySQLConnector::update(const std::string &sqlStr, MyVariants &params, 
	std::vector<int8_t>& types)
{
	try {
		std::unique_ptr<sql::PreparedStatement> stmt(m_con->prepareStatement(sqlStr));
		for (int i = 0; i < params.count(); ++i) {
			setParam(stmt.get(), i + 1, params.variant(i), types[i]);
		}
		int updateCount = stmt->executeUpdate();		
		return updateCount;
	}
	catch (sql::SQLException &e) {
		cerr << "Update Error: " << sqlStr << ":" << e.what() << endl;
		throw SQLConnectorException(e.what());
	}
}

int64_t MySQLConnector::insert(const std::string &sqlStr, MyVariants &params, 
	std::vector<int8_t>& types)
{
	int64_t newID = -1;
	try {
		std::unique_ptr<sql::PreparedStatement> stmt(m_con->prepareStatement(sqlStr));
		for (int i = 0; i < params.count(); ++i) {
			setParam(stmt.get(), i + 1, params.variant(i), types[i]);
		}

		stmt->executeUpdate();

		std::unique_ptr<sql::Statement> stmt2(m_con->createStatement());
		sql::ResultSet *res = stmt2->executeQuery("SELECT LAST_INSERT_ID() AS ID");
		if (res->next()) {
			newID = res->getInt64("ID");
		}
	}
	catch (sql::SQLException &e) {
		cerr << "Insert Error: " << sqlStr << ":" << e.what() << endl;
		throw SQLConnectorException(e.what());
	}
	return newID;
}

void MySQLConnector::startTransaction()
{
	m_con->setAutoCommit(false);
}

void MySQLConnector::commit()
{
	m_con->commit();
	m_con->setAutoCommit(true);
}

void MySQLConnector::rollBack()
{
	m_con->rollback();
	m_con->setAutoCommit(true);
}

void MySQLConnector::readSqlResult(sql::ResultSet* res, std::unordered_map<std::string, MyVariant>& values)
{
	auto metaData = res->getMetaData();
	int count = metaData->getColumnCount();
	for (int i = 0; i < count; ++i) {
		string name = metaData->getColumnLabel(i + 1);
		if (res->isNull(name)) {
			values[name] = nullptr;
			continue;
		}

		DataType type = sqlTypeToDataType(metaData->getColumnType(i + 1));
		switch (type)
		{
			case DataType::dtBoolean:
			{
				values[name] = res->getBoolean(name);
				break;
			}
			case DataType::dtSmallInt:
			case DataType::dtInt:
			{
				values[name] = res->getInt(name);
				break;
			}
			case DataType::dtBigInt:
			{
				values[name] = res->getInt64(name);
				break;
			}
			case DataType::dtFloat:
			case DataType::dtDouble:
			{
				values[name] = (double)res->getDouble(name);
				break;
			}
			case DataType::dtString:
			{
				values[name] = string(res->getString(name));
				break;
			}
			case DataType::dtBlob:
			{
				std::unique_ptr<std::istream> is(res->getBlob(name));
				is->seekg(0, is->end);
				int len = is->tellg();
				is->seekg(0, is->beg);
				ByteArray blobValue = ByteArray::from(len);
				is->read((char*)(blobValue->data()), len);
				values[name] = blobValue;
				break;
			}
			default:
				break;
			}
	}
}

void MySQLConnector::writeResultSchema(sql::ResultSetMetaData *metaData, WriteBuffer *buffer,
	std::vector<std::string> &columnNames, std::vector<DataType> &dataTypes)
{
	buffer->writeByte((int8_t)TableKind::tkNormal);
	buffer->writeString(metaData->getTableName(1));
	int colCount = metaData->getColumnCount();
	buffer->writeShort(colCount);
	for (int i = 0; i < colCount; ++i) {
		columnNames.push_back(metaData->getColumnName(i + 1));
		buffer->writeString(columnNames[i]);
		dataTypes.push_back(sqlTypeToDataType(metaData->getColumnType(i + 1)));
		buffer->writeByte((int8_t)dataTypes[i]);
		buffer->writeBoolean(true);
		buffer->writeBoolean(false);
		buffer->writeString("");
	}

	buffer->writeShort(0);
	buffer->writeInt(0);
}

void MySQLConnector::writeRecord(std::unordered_map<std::string, MyVariant> &sqlResult, WriteBuffer* buffer,
	const std::vector<std::string> &columnNames, const std::vector<DataType> &dataTypes)
{
	for (int i = 0; i < dataTypes.size(); ++i) {
		switch (dataTypes[i])
		{
		case DataType::dtBoolean:
		{
			buffer->writeBoolean(sqlResult[columnNames[i]].toBool());
			break;
		}
		case DataType::dtSmallInt:
		{
			buffer->writeShort(sqlResult[columnNames[i]].toInt());
			break;
		}
		case DataType::dtInt:
		{
			buffer->writeInt(sqlResult[columnNames[i]].toInt());
			break;
		}
		case DataType::dtBigInt:
		{
			buffer->writeLong(sqlResult[columnNames[i]].toInt64());
			break;
		}
		case DataType::dtFloat:
		case DataType::dtDouble:
		{
			buffer->writeDouble(sqlResult[columnNames[i]].toDouble());
			break;
		}
		case DataType::dtString:
		{
			buffer->writeString(sqlResult[columnNames[i]].toString());
			break;
		}
		case DataType::dtBlob:
		{
			buffer->writeBlock(sqlResult[columnNames[i]].toBlob());
			break;
		}
		default:
			break;
		}
	}
}

DataType MySQLConnector::sqlTypeToDataType(int type)
{
	switch (type) {
		case sql::DataType::TINYINT:
			return DataType::dtBoolean;
		case sql::DataType::SMALLINT: // 2bytes
		case sql::DataType::ENUM:
			return DataType::dtSmallInt;
		case sql::DataType::MEDIUMINT: // 3bytes
		case sql::DataType::INTEGER: // 4bytes
			return DataType::dtInt;
		case sql::DataType::BIGINT:
			return DataType::dtBigInt;
		case sql::DataType::REAL:
		case sql::DataType::DOUBLE:
			return DataType::dtDouble;
		case sql::DataType::BINARY:
		case sql::DataType::VARBINARY:
		case sql::DataType::LONGVARBINARY:
			return DataType::dtBlob;
		default:
			return DataType::dtString;
	}
}

DataType MySQLConnector::sqlTypeToDataType(const std::string& typeStr)
{
	if (typeStr == "tinyint") {
		return DataType::dtBoolean;
	}
	else if (typeStr == "smallint" || typeStr == "enum") {
		return DataType::dtSmallInt;
	}
	else if (typeStr == "mediumint" || typeStr == "int") {
		return DataType::dtInt;
	}
	else if (typeStr == "bigint") {
		return DataType::dtBigInt;
	}
	else if (typeStr == "real" || typeStr == "double") {
		return DataType::dtDouble;
	}
	else if (typeStr == "binary" || typeStr == "varbinary" || typeStr == "longvarbinary") {
		return DataType::dtBlob;
	}

	return DataType::dtString;
}

int MySQLConnector::dataTypeToSqlType(ParamDataType value)
{
	switch (value) {
		case ParamDataType::pdtString:
			return sql::DataType::VARCHAR;
		case ParamDataType::pdtBool:
			return sql::DataType::TINYINT;
		case ParamDataType::pdtInt:
			return sql::DataType::INTEGER;
		case ParamDataType::pdtLong:
			return sql::DataType::BIGINT;
		case ParamDataType::pdtDouble:
			return sql::DataType::DOUBLE;
		case ParamDataType::pdtBlob:
			return sql::DataType::VARBINARY;
		default:
			break;
	}

	return sql::DataType::UNKNOWN;
}

void MySQLConnector::setParam(sql::PreparedStatement *stmt, int index, const MyVariant& param, uint8_t type)
{
	if (param.isNull()) {
		stmt->setNull(index, dataTypeToSqlType((ParamDataType)type));
		return;
	}

	switch ((ParamDataType)type)
	{
		case ParamDataType::pdtString:
			stmt->setString(index, param.toString());
			break;
		case ParamDataType::pdtBool:
			stmt->setBoolean(index, param.toBool());
			break;
		case ParamDataType::pdtInt:
			stmt->setInt(index, param.toInt());
			break;
		case ParamDataType::pdtLong:
			stmt->setInt64(index, param.toInt64());
			break;
		case ParamDataType::pdtDouble:
			stmt->setDouble(index, param.toDouble());
			break;
		case ParamDataType::pdtBlob:
		{
			ByteArray v = param.toBlob();
			string strV((char*)(v->data()), v->byteLength());
			istringstream in(strV);
			stmt->setBlob(index, &in);
			break;
		}
		default:
			break;
	}
}
