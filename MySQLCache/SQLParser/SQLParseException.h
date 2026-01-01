#pragma once

#include <stdexcept>
#include <string>

class SQLParseException : public std::runtime_error
{
public:
	SQLParseException(const std::string &msg) : std::runtime_error(msg) {}
	SQLParseException(const char *msg) : std::runtime_error(msg) {}
};
