#pragma once

#include <string>
#include <cstdint>
#include <unordered_map>
#include "MyVariant.h"

extern const std::string SERVER_MODE;
extern const std::string SERVER_THREAD_COUNT;
extern const std::string WORKER_THREAD_COUNT;
extern const std::string ARRAY_MEMORY_LIMIT;
extern const std::string ARRAY_MEMORY_PUSH_LIMIT;
extern const std::string VAR_MEMORY_LIMIT;
extern const std::string VAR_MEMORY_PUSH_LIMIT;
extern const std::string TABLE_MEMORY_LIMIT;
extern const std::string TABLE_MEMORY_PUSH_LIMIT;
extern const std::string WRITE_BUFFER_DEFAULT_MEMORY;
extern const std::string MEMORY_ROOT_PATH;
extern const std::string SERVER_ADDR;
extern const std::string SQL_SERVER_ADDR;

class CacheSetting
{
public:
	CacheSetting(const std::string &path);
	~CacheSetting();

	MyVariant read(const std::string &name);

private:
	void readSetting(const std::string &path);
	MyVariant readValue(const std::string &value);
	
private:
	std::unordered_map<std::string, MyVariant> m_settings;
};
