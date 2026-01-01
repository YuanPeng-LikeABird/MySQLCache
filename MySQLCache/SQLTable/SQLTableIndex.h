#pragma once
#include "MyVariant.h"
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <unordered_set>

enum class TableIndexKind
{
	tikEqual = 0,
	tikGreater = 1,
	tikGreaterEqual = 2,
	tikLess = 3,
	tikLessEqual = 4,
	tikBetween = 5
};

struct TableIndexNode
{
	MyVariant key;
	std::vector<uint32_t> tableIds;
};

class SQLTableIndex;

class SQLTableIndexFactory
{
public:
	static SQLTableIndex *createIndex(TableIndexKind k);
};

class SQLTableIndex
{
public:
	virtual void add(const MyVariant &k, uint32_t tableId) = 0;
	virtual void add(const MyVariant &k1, const MyVariant &k2, uint32_t tableId) = 0;
	virtual void clear() = 0;
	virtual std::vector<uint32_t> find(const MyVariant &k) = 0;
};

class SQLTableEqualIndex : public SQLTableIndex
{
public:
	SQLTableEqualIndex();
	virtual ~SQLTableEqualIndex();

	void add(const MyVariant &k, uint32_t tableId) override;
	void add(const MyVariant &k1, const MyVariant &k2, uint32_t tableId) override;
	void clear() override;
	std::vector<uint32_t> find(const MyVariant &k) override;

private:
	std::unordered_map<MyVariant, std::vector<uint32_t>> m_index;
};

class SQLTableNoEqualIndex : public SQLTableIndex
{
public:
	SQLTableNoEqualIndex(bool isGreater, bool isEqual);
	virtual ~SQLTableNoEqualIndex();

	void add(const MyVariant &k, uint32_t tableId) override;
	void add(const MyVariant &k1, const MyVariant &k2, uint32_t tableId) override;
	void clear() override;
	std::vector<uint32_t> find(const MyVariant &k) override;

private:
	int locate(const MyVariant &v, bool &matched);

private:
	bool m_isGreater;
	bool m_isEqual;
	std::vector<TableIndexNode *> m_index;
};

class SQLTableBetweenIndex : public SQLTableIndex
{
public:
	SQLTableBetweenIndex();
	virtual ~SQLTableBetweenIndex();

	void add(const MyVariant &k, uint32_t tableId) override;
	void add(const MyVariant &k1, const MyVariant &k2, uint32_t tableId) override;
	void clear() override;
	std::vector<uint32_t> find(const MyVariant &k) override;

private:
	int locate(std::vector<TableIndexNode *> &index, const MyVariant &v, bool &matched);
	void add(std::vector<TableIndexNode *> &index, const MyVariant &k, uint32_t tableId);
	void find(bool isLow, const MyVariant &k, std::unordered_set<uint32_t> &tableIds);

private:
	std::vector<TableIndexNode *> m_lowIndex;
	std::vector<TableIndexNode *> m_highIndex;
};
