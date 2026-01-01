#include "SQLTableContainer.h"
#include "Common.h"
#include "SQLTable.h"
#include "MemoryManager.h"
#include "snappy.h"
#include "OutputStream.h"
#include "InputStream.h"
#include <algorithm>

using namespace std;

int g_containerSize = getCPUCount();
SQLTableContainer *g_containers = new SQLTableContainer[g_containerSize];
int g_containerIndex = 0;

const int32_t COMPRESS_RANGE_P = 7;
const int32_t COMPRESS_RANGE = 1 << COMPRESS_RANGE_P;

class ContainerGarbager
{
public:
	ContainerGarbager() {

	}

	~ContainerGarbager() {
		delete[] g_containers;
	}
};

ContainerGarbager g_containerGarbage;

SQLTableContainer::SQLTableContainer() :
	m_index(g_containerIndex++),
	m_used(0)
{
}

SQLTableContainer::~SQLTableContainer()
{
	reset();
}

SQLTableContainer *SQLTableContainer::instance(int index)
{
	return &g_containers[index];
}

void SQLTableContainer::reset()
{
	for (int i = 0; i < m_tables.size(); i += COMPRESS_RANGE) {
		if (m_isCompress[i >> COMPRESS_RANGE_P]) {
			uncompressTables(i);
		}

		for (int j = 0; j < COMPRESS_RANGE && i + j < m_tables.size(); ++j) {
			delete m_tables[i + j];
		}
	}
	m_tables.clear();
	m_isCompress.clear();
	m_visitCounts.clear();
	m_used = 0;
}

uint32_t SQLTableContainer::newTable(SQLTableSchema *schema, SQLTable **tablePtr)
{
	SQLTable *table = nullptr;
	if (schema->isGroupBy()) {
		table = new SQLReadOnlyTable(schema);
	}
	else if (schema->kind() == TableKind::tkNormal) {
		table = new SQLNormalTable(schema);
	}
	else if (schema->kind() == TableKind::tkJoin) {
		table = new SQLJoinTable(schema);
	}

	if (tablePtr) {
		*tablePtr = table;
	}

	m_tables.push_back(table);
	m_lastId = m_tables.size() - 1;
	if (m_lastId == (m_lastId >> COMPRESS_RANGE_P) << COMPRESS_RANGE_P) {
		m_isCompress.push_back(false);
		m_visitCounts.push_back(1);
	}
	return m_lastId;
}

SQLTable *SQLTableContainer::getTable(uint32_t id)
{
	if (id < m_tables.size()) {
		m_lastId = id;
		uint32_t rangeIndex = (m_lastId >> COMPRESS_RANGE_P) << COMPRESS_RANGE_P;
		m_visitCounts[rangeIndex]++;
		return assureTable(id);
	}

	return nullptr;
}

void SQLTableContainer::removeTable(uint32_t id)
{
	if (id < m_tables.size()) {
		SQLTable *tbl = assureTable(id);
		if (tbl) {
			m_used -= tbl->meomoryUsed();
			delete tbl;
		}
		m_tables[id] = nullptr;
	}
}

void SQLTableContainer::addMemoryUsed(int32_t mu)
{
	m_used += mu;
	if (m_used > MemoryManager::tableMemoryLimit()) {
		std::cout << "TABLE-MEMORY overflow warning: before" << m_used << std::endl;
		compressTables();
		std::cout << "TABLE-MEMORY overflow warning: after" << m_used << std::endl;
		// compress is still can't solve memory overflow, free some LRU tables
		if (m_used > MemoryManager::tableMemoryPushLimit()) {
			freeTables();
		}
	}
}

SQLTable *SQLTableContainer::assureTable(uint32_t id)
{
	// is compress blob
	if (m_isCompress[id >> COMPRESS_RANGE_P]) {
		uncompressTables((id >> COMPRESS_RANGE_P) << COMPRESS_RANGE_P);
	}
	
	return m_tables[id];
}

void SQLTableContainer::compressTables()
{
	OutputStream out;
	for (int i = 0; i < m_tables.size(); i += COMPRESS_RANGE) {
		if (i + COMPRESS_RANGE >= m_tables.size()) {
			break;
		}

		if (m_isCompress[i >> COMPRESS_RANGE_P]) {
			continue;
		}

		out.reset();
		uint32_t mms = 0;
		for (int index = 0; index < COMPRESS_RANGE; ++index) {
			SQLTable *tbl = m_tables[i + index];
			if (!tbl) {
				out.writeByte(-1);
				continue;
			}

			tbl->unload(out);
			mms += tbl->meomoryUsed();
			m_tables[i + index] = nullptr;
		}

		ByteArray srcData = out.toByteArray();
		string interData;
		int32_t dstLength = snappy::Compress((char *)srcData->data(), srcData->byteLength(), &interData);
		uint8_t *dstData = MemoryManager::instantce(m_index).allocate(dstLength + 4);
		copy(interData.begin(), interData.end(), dstData + 4);
		*((uint32_t *)dstData) = dstLength;

		m_tables[i] = reinterpret_cast<SQLTable *>(dstData);
		m_used -= mms - dstLength - 4;
		m_isCompress[i >> COMPRESS_RANGE_P] = true;
	}
}

void SQLTableContainer::uncompressTables(uint32_t index)
{
	uint8_t *srcData = reinterpret_cast<uint8_t *>(m_tables[index]);
	uint32_t srcLength = *((uint32_t *)srcData);
	string interData;
	snappy::Uncompress((char *)(srcData + 4), srcLength, &interData);
	MemoryManager::instantce(m_index).recycle(srcData);

	ByteArray uData = ByteArray::directFrom((uint8_t *)interData.data(), interData.length());
	InputStream in(uData);
	uint32_t mms = 0;
	for (int i = 0; i < COMPRESS_RANGE; ++i) {
		TableKind tk = (TableKind)in.readByte();
		SQLTable *tbl = nullptr;
		if (tk == TableKind::tkNormal) {
			tbl = new SQLNormalTable();
		}
		else if (tk == TableKind::tkJoin) {
			tbl = new SQLJoinTable();
		}
		else if (tk == TableKind::tkReadOnly) {
			tbl = new SQLReadOnlyTable();
		}
		else {
			// is removed
			continue;
		}

		int start = in.pos();
		tbl->load(in);
		int end = in.pos();
		mms += end - start;

		m_tables[index + i] = tbl;
	}

	m_used += mms - srcLength - 4;
	m_isCompress[index >> COMPRESS_RANGE_P] = false;
}

void SQLTableContainer::freeTables()
{
	vector<pair<uint32_t, uint32_t>> visitInfos;
	for (int i = 0; i < m_visitCounts.size(); ++i) {
		visitInfos.push_back(make_pair((uint32_t)i, m_visitCounts[i]));
	}

	sort(visitInfos.begin(), visitInfos.end(), [](pair<uint32_t, uint32_t> v1, pair<uint32_t, uint32_t> v2) {
		return v2.second > v1.second;
	});

	// range contains m_lastId can't be free, then choose min visitCount range
	uint32_t oldUsed = m_used;
	uint32_t lastRangeIndex = m_lastId >> COMPRESS_RANGE_P;
	for (int i = 0; i < visitInfos.size(); ++i) {
		uint32_t index = visitInfos[i].first >> COMPRESS_RANGE_P;
		if (lastRangeIndex == index) {
			continue;
		}

		if (m_isCompress[index]) {
			uint8_t *srcData = reinterpret_cast<uint8_t *>(m_tables[index]);
			uint32_t srcLength = *((uint32_t *)srcData);
			MemoryManager::instantce(m_index).recycle(srcData);
			m_tables[index] = nullptr;
			m_used -= srcLength + 4;
		}
		else {
			for (int j = 0; j < COMPRESS_RANGE; ++j) {
				index += j;
				if (index >= m_tables.size()) {
					break;
				}

				if (m_tables[index]) {
					m_used -= m_tables[index]->meomoryUsed();
					delete m_tables[index];
					m_tables[index] = nullptr;
				}
			}
		}

		if (m_used <= oldUsed >> 1) {
			break;
		}
	}

	for (int i = 0; i < m_visitCounts.size(); ++i) {
		m_visitCounts[i] = 0;
	}
}
