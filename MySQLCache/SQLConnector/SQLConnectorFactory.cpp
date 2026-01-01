#include "SQLConnectorFactory.h"
#include "MySQLConnector.h"

SQLConnector *SQLConnectorFactory::createConnector(const std::string &type)
{
	if (type == "mysql") {
		return new MySQLConnector();
	}

	return nullptr;
}
