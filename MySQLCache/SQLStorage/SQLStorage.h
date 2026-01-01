#pragma once
#include <string>
#include <map>
#include "ByteArray.h"

class SQLStorage
{
public:
	SQLStorage();
	~SQLStorage();

	void addSQLResult(const std::string &sqlMD5, ByteArray result);

private:
	std::map<std::string, ByteArray> m_sqlResults;
};
