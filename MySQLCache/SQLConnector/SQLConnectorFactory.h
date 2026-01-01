#pragma once
#include "SQLConnector.h"
#include <string>

class SQLConnectorFactory
{
public:
	static SQLConnector *createConnector(const std::string &type);
};
