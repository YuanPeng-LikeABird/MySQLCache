#include "VarMemoryManager.h"
#include "MemoryManager.h"
#include "snappy.h"
#include <filesystem>
#include <string>
#include <iostream>
#ifndef _WIN32
#include <cstring>
#endif

namespace fs = std::filesystem;

#pragma pack(1)
typedef struct VarBlockHeader
{
	uint32_t firstId;
	uint16_t varCount = 0;
	/*
	** 接下来是每个var的length，var的length用2字节保存
	** 新VarData从尾部插入
	** 为保证id连续不间断，被删除VarData只是var的length清为0并去除数据部分，但varCount不变
	*/ 
} *PVarBlockHeader;
#pragma pack()

#define MIN_BLOCK_LENGTH 100
#define MAX_VAR_LENGTH (m_blockByteCount - sizeof(VarBlockHeader) - 2)
#define BIG_VAR_MIN_ID 0x8000
#define ERROR_ID 0

const int32_t COMPRESS_RANGE_P = 7;
const int32_t COMPRESS_RANGE = 1 << COMPRESS_RANGE_P;

const int32_t PUSH_RANGE_P = 10;
const int32_t PUSH_RANGE = 1 << 10;

VarMemoryManager::VarMemoryManager(MemoryManager *owner) :
	m_allocator(owner),
	m_readPos(0),
	m_writePos(0),
	m_maxId(1),
	m_maxBigId(BIG_VAR_MIN_ID),
	m_used(0),
	m_swap(nullptr),
	m_inPush(false)
{
	m_blockByteCount = owner->regularMemoryLimit();
}

VarMemoryManager::~VarMemoryManager()
{
	if (m_swap) {
		fs::path filePath = m_swap->path().c_str();
		if (fs::exists(filePath)) {
			fs::remove(filePath);
		}
		delete m_swap;
	}
}

uint32_t VarMemoryManager::set(VarData data)
{
	if (data.len > MAX_VAR_LENGTH) {
		uint8_t *valuePtr = m_allocator->allocate(MyMax(data.len + 4, m_allocator->regularMemoryLimit()));
		*((uint32_t *)valuePtr) = data.len;
		memcpy(valuePtr + 4, data.data, data.len);
		m_bigVars[m_maxBigId] = valuePtr;
		m_used += data.len + 4;
		return m_maxBigId++;
	}

	if (data.len > m_dataCursor - m_headCursor + 2) {
		assureNewBlock();
	}

	PVarBlockHeader header = (PVarBlockHeader)m_blocks[m_writePos];
	if (header->varCount == 0) {
		header->firstId = m_maxId;
	}
	header->varCount++;
	*((uint16_t *)m_headCursor) = data.len;
	m_headCursor += 2;

	m_dataCursor -= data.len;
	memcpy(m_dataCursor, data.data, data.len);
	
	return m_maxId++;
}

uint32_t VarMemoryManager::set(uint32_t id, VarData data)
{
	if (id >= BIG_VAR_MIN_ID) {
		auto dataP = m_bigVars.find(id);
		uint8_t *valuePtr = nullptr;
		if (dataP != m_bigVars.end()) {
			valuePtr = dataP->second;
			uint32_t len = *((uint32_t *)valuePtr);
			if (len >= data.len) {
				*((uint32_t *)valuePtr) = data.len;
				memcpy(valuePtr + 4, data.data, data.len);
			}
			else {
				m_allocator->recycle(valuePtr);
				valuePtr = m_allocator->allocate(data.len + 4);
				*((uint32_t *)valuePtr) = data.len;
				memcpy(valuePtr + 4, data.data, data.len);
				m_bigVars[id] = valuePtr;
			}

			return id;
		}

		return ERROR_ID;
	}

	if (!moveReadPosToId(id)) {
		return ERROR_ID;
	}

	return write(id, data);
}

void VarMemoryManager::clear(uint32_t id)
{
	if (id >= BIG_VAR_MIN_ID) {
		auto dataP = m_bigVars.find(id);
		if (dataP != m_bigVars.end()) {
			uint8_t *valuePtr = dataP->second;
			uint32_t len = *((uint32_t *)valuePtr);
			m_allocator->recycle(valuePtr);
			m_used -= len + 4;
			m_bigVars.erase(dataP);
		}
	}
	else {
		VarData data;
		set(id, data);
	}
}

VarData VarMemoryManager::get(uint32_t id)
{
	VarData data;
	if (id >= BIG_VAR_MIN_ID) {
		auto dataP = m_bigVars.find(id);
		if (dataP != m_bigVars.end()) {
			data.len = *((uint32_t *)(*dataP->second));
			data.data = dataP->second + 4;
		}
	}
	else if (moveReadPosToId(id)) {
		data = read(id);
	}

	return data;
}

void VarMemoryManager::initialize()
{
	for (int i = 0; i < MIN_BLOCK_LENGTH; ++i) {
		m_blocks.push_back(m_allocator->allocate(m_blockByteCount));
		((PVarBlockHeader)m_blocks[i])->varCount = 0;
	}
	m_headCursor = m_blocks[0] + sizeof(VarBlockHeader);
	m_dataCursor = m_blocks[0] + m_blockByteCount;
}

void VarMemoryManager::reset()
{
	for (int i = 0; i < m_blocks.size(); i += COMPRESS_RANGE) {
		// is pushed to disk
		if (m_blocks[i] == nullptr) {
			continue;
		}

		// is already compress
		if (m_blocks[i + 1] == nullptr) {
			m_allocator->recycle(m_blocks[i]);
			continue;
		}

		for (int j = 0; j < COMPRESS_RANGE && i + j < m_blocks.size(); ++j) {
			m_allocator->recycle(m_blocks[i + j]);
		}
	}
	m_blocks.clear();
	m_readPos = 0;
	m_writePos = 0;
	m_maxId = 1;
	// normally m_bigArrays is free before, but still free it just in case
	for (auto i = m_bigVars.begin(); i != m_bigVars.end(); ++i) {
		m_allocator->recycle(i->second);
	}
	m_bigVars.clear();
	m_maxBigId = BIG_VAR_MIN_ID;

	m_used = 0;
	if (m_swap) {
		fs::path filePath = m_swap->path().c_str();
		if (fs::exists(filePath)) {
			fs::remove(filePath);
		}
		delete m_swap;
		m_swap = nullptr;
	}
	initialize();
}

void VarMemoryManager::pushBlock(PushBlockTaskData *data)
{
	if (!m_swap) {
		// is reseted
		m_inPush = false;
		delete data;
		return;
	}

	uint64_t pos = (uint64_t)data->start * m_blockByteCount;
	uint32_t count = 0;
	// push all to disk, the same as compress
	for (uint32_t i = data->start; i < data->total; i += COMPRESS_RANGE) {
		if (i + COMPRESS_RANGE > data->total - 1 || ++count > PUSH_RANGE) {
			break;
		}
		// is already push to block
		if (m_blocks[i] == nullptr) {
			continue;
		}
		// is compressed, uncompress and then push it
		if (m_blocks[i + 1] == nullptr) {
			uint8_t *srcData = m_blocks[i];
			uint32_t srcLength = *((uint32_t *)(srcData + 6));
			std::string interData;
			snappy::Uncompress((char *)(srcData + 10), srcLength, &interData);
			m_allocator->recycle(srcData);
			m_blocks[i] = nullptr;

			m_swap->write(pos, (uint8_t *)interData.c_str(), interData.length());
			pos += interData.length();
			m_used -= srcLength + 10;
			continue;
		}

		for (int j = 0; j < COMPRESS_RANGE; ++j) {
			uint8_t *srcData = m_blocks[i + j];
			m_swap->write(pos, srcData, m_blockByteCount);
			m_allocator->recycle(srcData);
			m_blocks[i + j] = nullptr;
			pos += m_blockByteCount;
		}
		m_used -= m_blockByteCount << COMPRESS_RANGE_P;
	}

	data->start += 1 >> (COMPRESS_RANGE_P + PUSH_RANGE_P);
	if (data->start + COMPRESS_RANGE > data->total - 1) {
		delete data;
		m_inPush = false;
		std::cout << "push var block end" << std::endl;
		return;
	}

	m_allocator->taskQueue()->addNewTask(TaskType::ttPushBlock, data);
}

void VarMemoryManager::assureNewBlock()
{
	if (++m_writePos >= m_blocks.size()) {
		m_blocks.push_back(m_allocator->allocate(m_blockByteCount));
	}
	m_headCursor = m_blocks[m_writePos] + sizeof(VarBlockHeader);
	m_dataCursor = m_blocks[m_writePos] + m_blockByteCount;
	((PVarBlockHeader)m_blocks[m_writePos])->varCount = 0;

	m_used += m_blockByteCount;
	if (m_used > MemoryManager::varMemoryLimit()) {
		std::cout << "overflow warning: before" << m_used << std::endl;
		compressBlock();
		std::cout << "overflow warning: after" << m_used << std::endl;
		// compress is still can't solve memory overflow, write to local disk
		if (m_used > MemoryManager::varMemoryPushLimit()) {
			preparePushBlock();
		}
	}
}

void VarMemoryManager::compressBlock()
{
	const uint32_t compressByteCount = m_blockByteCount << COMPRESS_RANGE_P;
	uint8_t *srcData = new uint8_t[compressByteCount];
	// compress all except the last one:because the last one is just allocated and soon be used
	// it is unnecessary to maintain LRU for getting knowledge for hot cache, because hot cache
	// will be hit and uncomress soon。since uncompress is quick, and after that, a long time is
	// uncompress data, is enough to keep efficient。 if maint LRU, the overhead memory and time
	// used is also not cheap, and most of all, the structure come to sophisticated.
	for (int i = 0; i < m_blocks.size(); i += COMPRESS_RANGE) {
		if (i + COMPRESS_RANGE > m_blocks.size() - 1) {
			break;
		}
		// is pushed to disk
		if (m_blocks[i] == nullptr) {
			continue;
		}
		// is already compress
		if (m_blocks[i + 1] == nullptr) {
			continue;
		}

		uint8_t *cur = srcData;
		uint32_t firstId = 0;
		uint16_t count = 0;
		for (int index = 0; index < COMPRESS_RANGE; ++index) {
			PVarBlockHeader header = (PVarBlockHeader)m_blocks[i + index];
			if (index == 0) {
				firstId = header->firstId;
			}
			count += header->varCount;

			memcpy(cur, m_blocks[i + index], m_blockByteCount);
			m_allocator->recycle(m_blocks[i + index]);
			m_blocks[i + index] = nullptr;
			cur += m_blockByteCount;
		}

		std::string interData;
		int32_t dstLength = snappy::Compress((char *)srcData, m_blockByteCount << COMPRESS_RANGE_P, &interData);
		uint8_t *dstData = m_allocator->allocate(dstLength + 10);
		std::copy(interData.begin(), interData.end(), dstData + 10);
		((PVarBlockHeader)dstData)->firstId = firstId;
		((PVarBlockHeader)dstData)->varCount = count;
		*((uint32_t *)(dstData + 6)) = dstLength;

		m_blocks[i] = dstData;
		m_used -= m_blockByteCount << COMPRESS_RANGE_P - dstLength - 10;
	}
	delete[] srcData;
}

void VarMemoryManager::uncompressBlock(uint32_t index)
{
	uint8_t *srcData = m_blocks[index];
	uint32_t srcLength = *((uint32_t *)(srcData + 6));
	std::string interData;
	snappy::Uncompress((char *)(srcData + 10), srcLength, &interData);
	m_allocator->recycle(srcData);

	for (int i = 0; i < COMPRESS_RANGE; ++i) {
		m_blocks[index + i] = m_allocator->allocate(m_blockByteCount);
		std::copy(interData.begin() + i * m_blockByteCount,
			interData.begin() + (i + 1) * m_blockByteCount, m_blocks[index + i]);
	}

	m_used += m_blockByteCount << COMPRESS_RANGE_P - srcLength - 10;
}

void VarMemoryManager::preparePushBlock()
{
	if (m_inPush) {
		return;
	}
	std::cout << "push var block begin" << std::endl;

	if (m_swap) {
		delete m_swap;
	}

	m_inPush = true;
	m_swap = new SwapFile(MemoryManager::rootPath() + "var" + std::to_string(m_allocator->index()) + ".dat",
		(uint64_t)(m_blocks.size() - 1) * m_blockByteCount);
	PushBlockTaskData *data = new PushBlockTaskData;
	data->start = 0;
	data->total = m_blocks.size();
	data->type = 2;
	m_allocator->taskQueue()->addNewTask(TaskType::ttPushBlock, data);
}

void VarMemoryManager::pullBlock(uint32_t index)
{
	uint64_t pos = (uint64_t)index * m_blockByteCount;
	for (int i = index; i < index + COMPRESS_RANGE; ++i) {
		if (m_swap->tryRead(pos, m_blockByteCount)) {
			m_blocks[i] = m_allocator->allocate(m_blockByteCount);
			m_swap->read(pos, m_blocks[i], m_blockByteCount);
			pos += m_blockByteCount;
			m_used += m_blockByteCount;
		}
		else {
			break;
		}
	}
}

bool VarMemoryManager::moveReadPosToId(uint32_t id)
{
	uint32_t oldReadPos = m_readPos;
	PVarBlockHeader curBlock = (PVarBlockHeader)m_blocks[m_readPos];
	bool isFind = false;
	int32_t begin = 0;
	int32_t end = m_blocks.size() - 1;
	while (begin <= end) {
		if (curBlock == nullptr) {
			// is compress block, find prev not-null block
			uint32_t readPos = (m_readPos >> COMPRESS_RANGE_P) << COMPRESS_RANGE_P;
			curBlock = (PVarBlockHeader)m_blocks[readPos];
			if (curBlock) {
				m_readPos = readPos;
			}
			else {
				// if block is still null, it must be pushed to disk, so pull it from disk to memory
				pullBlock(readPos);
				curBlock = (PVarBlockHeader)m_blocks[m_readPos];
			}
		}

		if (curBlock->firstId <= id && curBlock->firstId + curBlock->varCount > id) {
			// is compress block
			if (m_readPos < m_blocks.size() - 1 && m_blocks[m_readPos + 1] == nullptr) {
				uncompressBlock(m_readPos);
				begin = m_readPos;
				end = begin + COMPRESS_RANGE - 1;
				m_readPos = (begin + end) / 2;
				curBlock = (PVarBlockHeader)m_blocks[m_readPos];
				continue;
			}
			isFind = true;
			break;
		}

		if (curBlock->firstId > id) {
			end = m_readPos - 1;		
		}
		else {
			if (m_readPos < m_blocks.size() - 1 && m_blocks[m_readPos + 1] == nullptr) {
				begin = m_readPos + COMPRESS_RANGE;
			}
			else {
				begin = m_readPos + 1;
			}
		}

		m_readPos = (begin + end) / 2;
		curBlock = (PVarBlockHeader)m_blocks[m_readPos];
	}

	if (!isFind) {
		m_readPos = oldReadPos;
	}

	return isFind;
}

VarData VarMemoryManager::read(uint32_t id)
{
	VarData result;
	PVarBlockHeader curBlock = (PVarBlockHeader)m_blocks[m_readPos];
	if (curBlock->firstId > id || curBlock->firstId + curBlock->varCount <= id) {
		return result;
	}

	uint16_t *headerPtr = (uint16_t *)(m_blocks[m_readPos] + sizeof(VarBlockHeader));
	uint16_t offset = 0;
	for (uint32_t i = curBlock->firstId; i <= id; ++i) {
		result.len = *(headerPtr++);
		offset += result.len;
	}

	result.data = m_blocks[m_readPos] + m_blockByteCount - offset;
	return result;
}

uint32_t VarMemoryManager::write(uint32_t id, VarData data)
{
	PVarBlockHeader curBlock = (PVarBlockHeader)m_blocks[m_readPos];
	if (curBlock->firstId > id || curBlock->firstId + curBlock->varCount <= id) {
		return ERROR_ID;
	}

	uint16_t *headerPtr = (uint16_t *)(m_blocks[m_readPos] + sizeof(VarBlockHeader));
	uint16_t totalLen = 0;
	// length of all data after id
	uint16_t afterLen = 0;
	uint16_t oldDataLen = 0;
	for (uint32_t i = curBlock->firstId; i < curBlock->firstId + curBlock->varCount; ++i) {
		if (i == id) {
			oldDataLen = *(headerPtr++);
			totalLen += data.len;
		}
		else {
			uint16_t curLen = *(headerPtr++);
			totalLen += curLen;
			if (i > id) {
				afterLen += curLen;
			}
		}
	}
	// check current block is memory enough
	if (totalLen + curBlock->varCount * 2 + sizeof(VarBlockHeader) > m_blockByteCount) {
		return set(data);
	}
	// memory enough:ok
	headerPtr = (uint16_t*)(m_blocks[m_readPos] + sizeof(VarBlockHeader));
	headerPtr += id - curBlock->firstId;
	*headerPtr = data.len;

	uint8_t *dataPtr = m_blocks[m_readPos] + m_blockByteCount - (totalLen - data.len + oldDataLen);
	uint8_t *newDataPtr = m_blocks[m_readPos] + m_blockByteCount - totalLen;
	memmove(newDataPtr, dataPtr, afterLen);

	if (data.len > 0) {
		newDataPtr += afterLen;
		memcpy(newDataPtr, data.data, data.len);
	}
	
	m_dataCursor = newDataPtr;
	return id;
}
