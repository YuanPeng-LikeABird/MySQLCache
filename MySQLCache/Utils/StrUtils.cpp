#include "StrUtils.h"
#include "Common.h"
#include <algorithm>

std::string StrUtils::toUpper(const std::string &value)
{
	std::string result;
	result.resize(value.length());
	for (int i = 0; i < value.length(); ++i) {
		if (value[i] >= 'a' && value[i] <= 'z') {
			result[i] = value[i] - 32;
		}
		else {
			result[i] = value[i];
		}
	}
	return result;
}

std::string StrUtils::toLower(const std::string &value)
{
	std::string result;
	result.resize(value.length());
	for (int i = 0; i < value.length(); ++i) {
		if (value[i] >= 'A' && value[i] <= 'Z') {
			result[i] = value[i] + 32;
		}
		else {
			result[i] = value[i];
		}
	}
	return result;
}

bool StrUtils::startsWith(const std::string &value, const std::string &prefix)
{
	int n = prefix.size();
	if (value.size() < n) {
		return false;
	}

	for (int i = 0; i < n; ++i) {
		if (value[i] != prefix[i]) {
			return false;
		}
	}
	return true;
}

std::vector<std::string> StrUtils::split(const std::string &str, char delim)
{
	std::size_t previous = 0;
	std::size_t current = str.find(delim);
	std::vector<std::string> elems;
	while (current != std::string::npos) {
		if (current >= previous) {
			elems.push_back(str.substr(previous, current - previous));
		}
		previous = current + 1;
		current = str.find(delim, previous);
	}

	if (previous != str.size()) {
		elems.push_back(str.substr(previous));
	}
	return elems;
}

std::string StrUtils::trim(const std::string &str)
{
	std::string::size_type start = str.find_first_not_of(" \r\n\t");
	if (start == std::string::npos) {
		return "";
	}
	std::string::size_type end = str.find_last_not_of(" \r\n\t");
	return str.substr(start, end - start + 1);
}

std::string StrUtils::replace(const std::string& str, const std::string& src, const std::string& dst)
{
	std::string result = str;
	std::string::size_type pos;
	std::string::size_type start = 0;
	while ((pos = result.find(src, start)) != std::string::npos)
	{
		result = result.substr(0, pos).append(dst).append(result.substr(pos + src.length()));
		start = pos + dst.length();
	}

	return result;
}
