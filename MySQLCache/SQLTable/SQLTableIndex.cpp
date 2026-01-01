#include "SQLTableIndex.h"
#include "Common.h"

using namespace std;

SQLTableIndex *SQLTableIndexFactory::createIndex(TableIndexKind k)
{
	switch (k) {
	case TableIndexKind::tikEqual:
		return new SQLTableEqualIndex();
	case TableIndexKind::tikGreater:
		return new SQLTableNoEqualIndex(true, false);
	case TableIndexKind::tikGreaterEqual:
		return new SQLTableNoEqualIndex(true, true);
	case TableIndexKind::tikLess:
		return new SQLTableNoEqualIndex(false, false);
	case TableIndexKind::tikLessEqual:
		return new SQLTableNoEqualIndex(false, true);
	case TableIndexKind::tikBetween:
		return new SQLTableBetweenIndex();
	default:
		break;
	}
	return nullptr;
}

SQLTableEqualIndex::SQLTableEqualIndex()
{
}

SQLTableEqualIndex::~SQLTableEqualIndex()
{
}

void SQLTableEqualIndex::add(const MyVariant &k, uint32_t tableId)
{
	auto r = m_index.find(k);
	if (r == m_index.end()) {
		m_index[k] = vector<uint32_t>();
	}

	m_index[k].push_back(tableId);
}

void SQLTableEqualIndex::add(const MyVariant &k1, const MyVariant &k2, uint32_t tableId)
{
	// do nothing
}

void SQLTableEqualIndex::clear()
{
	m_index.clear();
}

std::vector<uint32_t> SQLTableEqualIndex::find(const MyVariant &k)
{
	auto r = m_index.find(k);
	if (r != m_index.end()) {
		return r->second;
	}

	return std::vector<uint32_t>();
}

SQLTableNoEqualIndex::SQLTableNoEqualIndex(bool isGreater, bool isEqual) :
	m_isGreater(isGreater),
	m_isEqual(isEqual)
{
}

SQLTableNoEqualIndex::~SQLTableNoEqualIndex()
{
	for (int i = 0; i < m_index.size(); ++i) {
		delete m_index[i];
	}
}

void SQLTableNoEqualIndex::add(const MyVariant &k, uint32_t tableId)
{
	bool matched = false;
	int i = locate(k, matched);
	if (!matched) {
		TableIndexNode *node = new TableIndexNode;
		node->key = k;
		m_index.insert(m_index.begin() + i, node);
	}
	m_index[i]->tableIds.push_back(tableId);
}

void SQLTableNoEqualIndex::add(const MyVariant &k1, const MyVariant &k2, uint32_t tableId)
{
	// do nothing
}

void SQLTableNoEqualIndex::clear()
{
	for (int i = 0; i < m_index.size(); ++i) {
		delete m_index[i];
	}
	m_index.clear();
}

std::vector<uint32_t> SQLTableNoEqualIndex::find(const MyVariant &k)
{
	vector<uint32_t> result;
	bool matched = false;
	int i = locate(k, matched);
	int s = -1;
	int e = -1;
	if (m_isGreater) {
		s = i;
		e = m_index.size() - 1;
		if (matched && !m_isEqual) {
			++s;
		}
	}
	else {
		s = 0;
		e = i;
		if (!matched || !m_isEqual) {
			--e;
		}
	}
	for (i = s; i <= e; ++i) {
		result.insert(result.end(), m_index[i]->tableIds.begin(), m_index[i]->tableIds.end());
	}
	return result;
}

int SQLTableNoEqualIndex::locate(const MyVariant &v, bool &matched)
{
	int left = 0;
	int right = m_index.size() - 1;
	while (left <= right) { 
		int mid = left + (right - left) / 2;
		if (m_index[mid]->key == v) {
			matched = true;
			return mid;
		}
		else if (m_index[mid]->key < v) {
			left = mid + 1; 
		}
		else { 
			right = mid - 1;
		}
	}

	matched = false;
	return left;
}

SQLTableBetweenIndex::SQLTableBetweenIndex()
{
}

SQLTableBetweenIndex::~SQLTableBetweenIndex()
{
	for (int i = 0; i < m_lowIndex.size(); ++i) {
		delete m_lowIndex[i];
	}
	for (int i = 0; i < m_highIndex.size(); ++i) {
		delete m_highIndex[i];
	}
}

void SQLTableBetweenIndex::add(const MyVariant &k, uint32_t tableId)
{
	// do nothing
}

void SQLTableBetweenIndex::add(const MyVariant &k1, const MyVariant &k2, uint32_t tableId)
{
	add(m_lowIndex, k1, tableId);
	add(m_highIndex, k2, tableId);
}

void SQLTableBetweenIndex::clear()
{
	for (int i = 0; i < m_lowIndex.size(); ++i) {
		delete m_lowIndex[i];
	}
	m_lowIndex.clear();

	for (int i = 0; i < m_highIndex.size(); ++i) {
		delete m_highIndex[i];
	}
	m_highIndex.clear();
}

std::vector<uint32_t> SQLTableBetweenIndex::find(const MyVariant &k)
{
	unordered_set<uint32_t> lowTableIds;
	find(true, k, lowTableIds);

	unordered_set<uint32_t> highTableIds;
	find(false, k, highTableIds);

	vector<uint32_t> result;
	FOR_EACH(i, lowTableIds) {
		if (highTableIds.find(*i) != highTableIds.end()) {
			result.push_back(*i);
		}
	}
	return result;
}

int SQLTableBetweenIndex::locate(std::vector<TableIndexNode *> &index, 
	const MyVariant &v, bool &matched)
{
	int left = 0;
	int right = index.size() - 1;
	while (left <= right) {
		int mid = left + (right - left) / 2;
		if (index[mid]->key == v) {
			matched = true;
			return mid;
		}
		else if (index[mid]->key < v) {
			left = mid + 1;
		}
		else {
			right = mid - 1;
		}
	}

	matched = false;
	return left;
}

void SQLTableBetweenIndex::add(std::vector<TableIndexNode *> &index, 
	const MyVariant &k, uint32_t tableId)
{
	bool matched = false;
	int i = locate(index, k, matched);
	if (!matched) {
		TableIndexNode *node = new TableIndexNode;
		node->key = k;
		index.insert(index.begin() + i, node);
	}
	index[i]->tableIds.push_back(tableId);
}

void SQLTableBetweenIndex::find(bool isLow, const MyVariant &k, 
	std::unordered_set<uint32_t> &tableIds)
{
	vector<TableIndexNode *> &index = isLow ? m_lowIndex : m_highIndex;
	bool matched = false;
	int i = locate(index, k, matched);
	int s = -1;
	int e = -1;
	if (isLow) {
		s = i;
		e = index.size() - 1;
	}
	else {
		s = 0;
		e = i;
	}

	for (i = s; i <= e; ++i) {
		tableIds.insert(index[i]->tableIds.begin(), index[i]->tableIds.end());
	}
}
