#include "CacheSetting.h"
#include "StrUtils.h"
#include <fstream>
#include <iostream>

const std::string SERVER_MODE = "server-mode";
const std::string SERVER_THREAD_COUNT = "server-thread-count";
const std::string WORKER_THREAD_COUNT = "worker-thread-count";
const std::string ARRAY_MEMORY_LIMIT = "array-memory-limit";
const std::string ARRAY_MEMORY_PUSH_LIMIT = "array-memory-push-limit";
const std::string VAR_MEMORY_LIMIT = "var-memory-limit";
const std::string VAR_MEMORY_PUSH_LIMIT = "var-memory-push-limit";
const std::string TABLE_MEMORY_LIMIT = "table-memory-limit";
const std::string TABLE_MEMORY_PUSH_LIMIT = "table-memory-push-limit";
const std::string WRITE_BUFFER_DEFAULT_MEMORY = "write-buffer-default-memory";
const std::string MEMORY_ROOT_PATH = "memory-root-path";
const std::string SERVER_ADDR = "server-addr";
const std::string SQL_SERVER_ADDR = "sql-server-addr";

using namespace std;

CacheSetting::CacheSetting(const std::string &path)
{
	readSetting(path);
}

CacheSetting::~CacheSetting()
{
}

MyVariant CacheSetting::read(const std::string &name)
{
	auto i = m_settings.find(name);
	if (i != m_settings.end()) {
		return i->second;
	}

	return MyVariant();
}

void CacheSetting::readSetting(const std::string &path)
{
	ifstream in(path);
	if (!in.is_open()) {
		cout << "Setting File: " << path << " is not readable" << endl;
		return;
	}

	string line;
	while (getline(in, line)) { // 读取一行并存储到line中
		line = StrUtils::trim(line);
		int m = line.find(':');
		if (m <= 0) {
			continue;
		}

		string name = line.substr(0, m);
		string value = line.substr(m + 1);
		m_settings[name] = readValue(value);
	}
}

MyVariant CacheSetting::readValue(const std::string &value)
{
	if (value == "true") {
		return true;
	}
	else if (value == "false") {
		return false;
	}
	else if (value[0] >= '0' && value[0] <= '9') {
		int n = 0;
		for (int i = 0; i < value.size(); ++i) {
			if (value[i] == '.') {
				++n;
			}
			else if (value[i] < '0' || value[i] > '9') {
				return value;
			}
		}
		
		if (n == 0) {
			return atoi(value.c_str());
		}
		else if (n == 1) {
			return atof(value.c_str());
		}
	}
	
	return value;
}
