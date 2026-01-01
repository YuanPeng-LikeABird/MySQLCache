#include "CacheMonitor.h"
#include <iostream>
#include <sstream>

using namespace std;

CacheMonitor g_monitor;

CacheMonitor::CacheMonitor() :
	m_hitInfos(nullptr)
{
}

CacheMonitor::~CacheMonitor()
{
	if (m_hitInfos) {
		delete[] m_hitInfos;
	}
}

CacheMonitor *CacheMonitor::instance()
{
	return &g_monitor;
}

void CacheMonitor::init(int num)
{
	m_thNum = num;
	m_hitInfos = new HitInfo[m_thNum];
}

void CacheMonitor::writeHit(const std::string &sql, int index)
{
	if (index >= m_thNum) {
		std::cout << "writeHit error : index(" << index << ") thNum(" << m_thNum << ")" << std::endl;
		return;
	}

	auto values = m_hitInfos[index].find(sql);
	if (values == m_hitInfos[index].end()) {
		m_hitInfos[index].emplace(sql, 1);
	}
	else {
		++values->second;
	}
}

std::string CacheMonitor::outputHitInfo()
{
	ostringstream out;
	for (int i = 0; i < m_thNum; ++i) {
		for (auto j = m_hitInfos[i].begin(); j != m_hitInfos[i].end(); ++j) {
			out << j->first << ":" << to_string(j->second) << "\r\n";
		}
	}
	return out.str();
}
