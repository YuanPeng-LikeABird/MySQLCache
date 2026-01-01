#pragma once

#include <unordered_map>
#include <string>
#include <cstdint>

typedef std::unordered_map<std::string, uint64_t> HitInfo;

class CacheMonitor
{
public:
	CacheMonitor();
	~CacheMonitor();

	static CacheMonitor *instance();

	void init(int num);

	void writeHit(const std::string &sql, int index);
	std::string outputHitInfo();

private:
	HitInfo *m_hitInfos;
	int m_thNum;
};
