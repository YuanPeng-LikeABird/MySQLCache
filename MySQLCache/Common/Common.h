#pragma once

#include <string>

struct Range
{
	int start = -1;
	int end = -1;
};

#define MyMax(a, b) (a)>(b)?(a):(b)

#define FOR_EACH(i, els) for (auto i = els.begin(); i != els.end(); ++i)

#define COMPARE_RESULT(v1, v2, order) if (v2 > v1) { \
if (order == OrderType::otAsc) { \
	return true; \
} \
} \
else if (v2 < v1) { \
	if (order == OrderType::otDesc) { \
		return true; \
	} \
}

int getCPUCount();

std::string exePath();

enum SQLCacheErrorCode
{
	scecNone = 0,
	scecInvalidSql = 1,
	scecInvalidCacheSql = 2,
	scecSqlFail = 3,
	scecWriteServerError = 4,
	scecServerError = 5
};

enum class CommandType
{
	ctUnknown = 0,
	ctSelect = 1,
	ctInsert = 2,
	ctDelete = 3,
	ctUpdate = 4,
	ctMonitor = 5,
	ctConfirmWriteNode = 6,
	ctReset = 7,
	ctConnectSqlServer = 8,
	ctStartTransaction = 9,
	ctCommit = 10
};

CommandType parseCommandType(const std::string &sqlStr);

enum class ParamDataType
{
	pdtString = 1,
	pdtBool = 2,
	pdtInt = 3,
	pdtLong = 4,
	pdtDouble = 5,
	pdtBlob = 6
};
