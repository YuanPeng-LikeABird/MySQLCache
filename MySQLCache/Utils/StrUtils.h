#pragma once

#include <string>
#include <vector>

class StrUtils
{
public:
	static std::string toUpper(const std::string &value);
	static std::string toLower(const std::string &value);
	static bool startsWith(const std::string &value, const std::string &prefix);
	static std::vector<std::string> split(const std::string &str, char delim);
	static std::string trim(const std::string &str);
	static std::string replace(const std::string& str, const std::string& src, const std::string& dst);
	template<class ...Args>
	static std::string join(Args... strs)
	{
		std::string result;
		int dummy[sizeof...(strs)] = { (result.append(strs), 0)... };
		return result;
	}

	template<class ...Args>
	static void append(std::string &src, Args... strs)
	{
		int dummy[sizeof...(strs)] = { (src.append(strs), 0)... };
	}
};
