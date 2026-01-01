#include "SQLStorage.h"

SQLStorage::SQLStorage()
{
}

SQLStorage::~SQLStorage()
{
}

void SQLStorage::addSQLResult(const std::string &sqlMD5, ByteArray result)
{
	m_sqlResults[sqlMD5] = result;
}
