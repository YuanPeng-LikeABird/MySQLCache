#pragma once
#include <cstdint>
#include <vector>

class SQLTable;
class SQLTableSchema;

class SQLTableContainer
{
public:
	SQLTableContainer();
	~SQLTableContainer();

	static SQLTableContainer *instance(int index = 0);

	void reset();

	uint32_t newTable(SQLTableSchema *schema, SQLTable **tablePtr = nullptr);
	SQLTable *getTable(uint32_t id);
	void removeTable(uint32_t id);

	void addMemoryUsed(int32_t mu);

private:
	SQLTable *assureTable(uint32_t id);

	void compressTables();
	void uncompressTables(uint32_t index);

	void freeTables();

private:
	uint8_t m_index;
	std::vector<SQLTable *> m_tables;
	std::vector<uint32_t> m_visitCounts;
	std::vector<bool> m_isCompress;
	uint32_t m_lastId;
	uint64_t m_used;
};
