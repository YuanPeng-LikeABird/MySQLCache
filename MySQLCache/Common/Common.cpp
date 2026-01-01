#include "Common.h"
#include "StrUtils.h"
#ifdef _WIN32
#include <Windows.h>
#else
#include <unistd.h>
#include <sys/sysinfo.h>
#include <limits.h>
#include <libgen.h>
#endif

int getCPUCount()
{
#ifdef _WIN32
	SYSTEM_INFO sysInfo;
	GetSystemInfo(&sysInfo);
	int result = sysInfo.dwNumberOfProcessors;
#else
	int result = sysconf(_SC_NPROCESSORS_CONF);
#endif
	if (result <= 4) {
		result = 4;
	}
	else if (result <= 8) {
		result = 8;
	}
	else if (result <= 16) {
		result = 16;
	}
	else if (result <= 32) {
		result = 32;
	}

	return result;
}

std::string exePath()
{
#ifdef _WIN32
	char szModuleName[MAX_PATH + 1] = { 0 };
	GetModuleFileNameA(NULL, szModuleName, MAX_PATH + 1);
	std::string strPath(szModuleName);
	int nIndex = strPath.find_last_of('\\');
	if (nIndex < 0)
	{
		nIndex = strPath.find_last_of('/');
		if (nIndex < 0)
		{
			return "";
		}
	}
	strPath = strPath.substr(0, nIndex);
	return strPath;
#else
	char result[PATH_MAX];
	readlink("/proc/self/exe", result, PATH_MAX);
	char *path = dirname(result);
	return path;
#endif
}

CommandType parseCommandType(const std::string &sqlStr)
{
	std::string sql = StrUtils::trim(sqlStr);
	std::string::size_type n = sql.find_first_of(" \r\n\t");
	if (n == std::string::npos) {
		n = sqlStr.length();
	}

	std::string typeStr = StrUtils::toUpper(sql.substr(0, n));
	if (typeStr == "SELECT") {
		return CommandType::ctSelect;
	}
	else if (typeStr == "UPDATE") {
		return CommandType::ctUpdate;
	}
	else if (typeStr == "DELETE") {
		return CommandType::ctDelete;
	}
	else if (typeStr == "INSERT") {
		return CommandType::ctInsert;
	}
	else if (typeStr == "MONITOR") {
		return CommandType::ctMonitor;
	}
	else if (typeStr == "RESET") {
		return CommandType::ctReset;
	}
	else if (typeStr == "COMMIT") {
		return CommandType::ctCommit;
	}
	else if (typeStr == "CONNECT") {
		return CommandType::ctConnectSqlServer;
	}
	else if (typeStr == "I_AM_WRITE_NODE") {
		return CommandType::ctConfirmWriteNode;
	}
	else if (typeStr == "START" || typeStr == "BEGIN") {
		return CommandType::ctStartTransaction;
	}


	return CommandType::ctUnknown;
}
