#pragma once

#include <stdexcept>
#include <string>

class SQLConnectorException : public std::runtime_error
{
public:
	SQLConnectorException(const std::string &msg) : std::runtime_error(msg) {}
	SQLConnectorException(const char *msg) : std::runtime_error(msg) {}
};
