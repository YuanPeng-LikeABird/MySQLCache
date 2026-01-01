#include "ArrayMemoryManager.h"
#include "MemoryManager.h"
#include "snappy.h"
#include "MyException.h"
#include <filesystem>
#include <string>
#include <iostream>
#ifndef _WIN32
#include <cstring>
#endif

namespace fs = std::filesystem;

#pragma pack(1)
typedef struct ArrayBlockHeader
{
	uint32_t firstId;
	uint16_t arrayCount = 0;
	uint16_t used = 0;  // used byte count
	/*
	** 接下来是每个array的length，array的length用2字节保存
	** 新array从尾部插入
	** 为保证id连续不间断，被删除array只是length清为0并去除数据部分，但arrayCount不变
	** 当block处于写游标之前(写满过),为利用被删除空间，但被删除空间大小达到一定字节数，把该block用
	** 名为空闲block链表存起来，当下次出现所有block被全部写完，要分配新block时，优先利用空闲block
	** 链表里的空间。要保证已分配数据的id不变，那么插入空闲链表block的记录只能复用之前被删除的id
	** 空闲链表的策略不适合VarMemory的原因：是Var的修改大小会变，可能变大，最好给变大预留空间
	*/
} *PArrayBlockHeader;
#pragma pack()

#define MIN_BLOCK_LENGTH 1000
#define REUSE_BYTE_LIMIT 4096
#define MAX_ARRAY_LENGTH (m_blockByteCount - sizeof(ArrayBlockHeader) - 2)
#define BIG_ARRAY_MIN_ID 0xA000
#define ERROR_ID 0

const int32_t COMPRESS_RANGE_P = 7;
const int32_t COMPRESS_RANGE = 1 << COMPRESS_RANGE_P;

const int32_t PUSH_RANGE_P = 10;
const int32_t PUSH_RANGE = 1 << 10;

ArrayMemoryManager::ArrayMemoryManager(MemoryManager *owner) :
	m_allocator(owner),
	m_readPos(0),
	m_writePos(0),
	m_maxId(1),
	m_maxBigId(BIG_ARRAY_MIN_ID),
	m_operator(this),
	m_used(0),
	m_swap(nullptr),
	m_inPush(false)
{
	m_blockByteCount = owner->regularMemoryLimit();
}

ArrayMemoryManager::~ArrayMemoryManager()
{
	if (m_swap) {
		fs::path filePath = m_swap->path().c_str();
		if (fs::exists(filePath)) {
			fs::remove(filePath);
		}
		delete m_swap;
	}
}

uint32_t ArrayMemoryManager::allocate(uint32_t len)
{
	if (len > MAX_ARRAY_LENGTH) {
		uint8_t *valuePtr = m_allocator->allocate(MyMax(len + 4, m_allocator->regularMemoryLimit() + 1));
		*((uint32_t *)valuePtr) = len;
		m_bigArrays[m_maxBigId] = valuePtr;
		m_used += len + 4;
		return m_maxBigId++;
	}

	if (len < REUSE_BYTE_LIMIT && m_reuseBlocks.size() > 0) {
		uint32_t id = allocateInReuse(len);
		if (id > 0) {
			return id;
		}
	}

	if (len > m_dataCursor - m_headCursor + 2) {
		assureNewBlock();
	}

	PArrayBlockHeader header = (PArrayBlockHeader)m_blocks[m_writePos];
	if (header->arrayCount == 0) {
		header->firstId = m_maxId;
	}
	header->arrayCount++;
	header->used += len;
	*((uint16_t *)m_headCursor) = len;
	m_headCursor += 2;

	m_dataCursor -= len;
	return m_maxId++;
}

void ArrayMemoryManager::recycle(uint32_t id)
{
	if (id >= BIG_ARRAY_MIN_ID) {
		auto i = m_bigArrays.find(id);
		if (i != m_bigArrays.end()) {
			m_used -= *((uint32_t *)(i->second)) + 4;
			m_allocator->recycle(i->second);
			m_bigArrays.erase(i);
		}
		return;
	}

	moveReadPosToId(id);
	PArrayBlockHeader curBlock = (PArrayBlockHeader)m_blocks[m_readPos];
	if (curBlock->firstId > id || curBlock->firstId + curBlock->arrayCount <= id) {
		return;
	}

	uint16_t *headerPtr = (uint16_t *)(m_blocks[m_readPos] + sizeof(ArrayBlockHeader));
	// length of all data after id
	uint16_t afterLen = 0;
	headerPtr += id - curBlock->firstId;
	uint16_t dataLen = *headerPtr;
	*headerPtr = 0;
	for (uint32_t i = id + 1; i < curBlock->firstId + curBlock->arrayCount; ++i) {
		afterLen += *(++headerPtr);
	}
	
	if (dataLen > 0) {
		uint8_t *dataPtr = m_blocks[m_readPos] + m_blockByteCount - curBlock->used;
		memmove(dataPtr + dataLen, dataPtr, afterLen);
		curBlock->used -= dataLen;
	}

	if (m_readPos < m_writePos) {
		uint16_t avail = m_blockByteCount - (curBlock->arrayCount * 2 + curBlock->used);
		if (avail >= REUSE_BYTE_LIMIT) {
			addReuseBlock(m_readPos);
		}
	}
}

std::string ArrayMemoryManager::used() const
{
	std::string usedInfo;
	usedInfo += "Total Used: ";
	usedInfo += std::to_string(m_used);
	usedInfo += ";";

	uint64_t compressUsed = 0;
	for (int i = 0; i < m_blocks.size(); i += COMPRESS_RANGE) {
		if (m_blocks[i] == nullptr) {
			continue;
		}

		if (m_blocks[i + 1] == nullptr) {
			compressUsed += *((uint32_t *)(m_blocks[i] + 6));
		}
	}
	usedInfo += "Compress Used: ";
	usedInfo += std::to_string(compressUsed);
	usedInfo += ";";
	return usedInfo;
}

ArrayMemoryManager::ArrayMemoryOperator &ArrayMemoryManager::memoryOperator(uint32_t id)
{
	if (id > 0) {
		m_operator.setID(id);
	}
	
	return m_operator;
}

void ArrayMemoryManager::initialize()
{
	for (int i = 0; i < MIN_BLOCK_LENGTH; ++i) {
		m_blocks.push_back(m_allocator->allocate(m_blockByteCount));
		((PArrayBlockHeader)m_blocks[i])->used = 0;
		((PArrayBlockHeader)m_blocks[i])->arrayCount = 0;
	}
	m_headCursor = m_blocks[0] + sizeof(ArrayBlockHeader);
	m_dataCursor = m_blocks[0] + m_blockByteCount;
}

void ArrayMemoryManager::reset()
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
	for (auto i = m_bigArrays.begin(); i != m_bigArrays.end(); ++i) {
		m_allocator->recycle(i->second);
	}
	m_bigArrays.clear();
	m_maxBigId = BIG_ARRAY_MIN_ID;

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

void ArrayMemoryManager::assureNewBlock()
{
	if (++m_writePos >= m_blocks.size()) {
		m_blocks.push_back(m_allocator->allocate(m_blockByteCount));
	}
	m_headCursor = m_blocks[m_writePos] + sizeof(ArrayBlockHeader);
	m_dataCursor = m_blocks[m_writePos] + m_blockByteCount;
	((PArrayBlockHeader)m_blocks[m_writePos])->used = 0;
	((PArrayBlockHeader)m_blocks[m_writePos])->arrayCount = 0;
	m_used += m_blockByteCount;
	if (m_used > MemoryManager::arrayMemoryLimit()) {
		std::cout << "overflow warning: before" << m_used << std::endl;
		compressBlock();
		std::cout << "overflow warning: after" << m_used << std::endl;
		// compress is still can't solve memory overflow, write to local disk
		if (m_used > MemoryManager::arrayMemoryPushLimit()) {
			preparePushBlock();
		}
	}
}

void ArrayMemoryManager::compressBlock()
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
			PArrayBlockHeader header = (PArrayBlockHeader)m_blocks[i + index];
			if (index == 0) {
				firstId = header->firstId;
			}
			count += header->arrayCount;

			memcpy(cur, m_blocks[i + index], m_blockByteCount);
			m_allocator->recycle(m_blocks[i + index]);
			m_blocks[i + index] = nullptr;
			cur += m_blockByteCount;
		}

		std::string interData;
		int32_t dstLength = snappy::Compress((char *)srcData, m_blockByteCount << COMPRESS_RANGE_P, &interData);
		uint8_t *dstData = m_allocator->allocate(dstLength + 10);
		std::copy(interData.begin(), interData.end(), dstData + 10);
		((PArrayBlockHeader)dstData)->firstId = firstId;
		((PArrayBlockHeader)dstData)->arrayCount = count;
		*((uint32_t *)(dstData + 6)) = dstLength;

		m_blocks[i] = dstData;
		m_used -= m_blockByteCount << COMPRESS_RANGE_P - dstLength - 10;
	}
	delete[] srcData;

	m_operator.setID(ERROR_ID);
}

void ArrayMemoryManager::uncompressBlock(uint32_t index)
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

void ArrayMemoryManager::preparePushBlock()
{
	if (m_inPush) {
		return;
	}
	std::cout << "push array block begin" << std::endl;

	if (m_swap) {
		delete m_swap;
	}

	m_inPush = true;
	m_swap = new SwapFile(MemoryManager::rootPath() + "array" + std::to_string(m_allocator->index()) + ".dat",
		(uint64_t)(m_blocks.size() - 1) * m_blockByteCount);
	PushBlockTaskData *data = new PushBlockTaskData;
	data->start = 0;
	data->total = m_blocks.size();
	data->type = 1;
	m_allocator->taskQueue()->addNewTask(TaskType::ttPushBlock, data);
}

void ArrayMemoryManager::pushBlock(PushBlockTaskData *data)
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
		std::cout << "push array block end" << std::endl;
		return;
	}

	m_allocator->taskQueue()->addNewTask(TaskType::ttPushBlock, data);
}
// index must be the pos where COMPRESS_RANGE start at
void ArrayMemoryManager::pullBlock(uint32_t index)
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

bool ArrayMemoryManager::moveReadPosToId(uint32_t id)
{
	uint32_t oldReadPos = m_readPos;
	PArrayBlockHeader curBlock = (PArrayBlockHeader)m_blocks[m_readPos];
	bool isFind = false;
	int32_t begin = 0;
	int32_t end = m_blocks.size() - 1;
	while (begin <= end) {
		if (curBlock == nullptr) {
			// is compress block, find prev not-null block
			uint32_t readPos = (m_readPos >> COMPRESS_RANGE_P) << COMPRESS_RANGE_P;
			curBlock = (PArrayBlockHeader)m_blocks[readPos];
			if (curBlock) {
				m_readPos = readPos;
			}
			else {
				// if block is still null, it must be pushed to disk, so pull it from disk to memory
				pullBlock(readPos);
				curBlock = (PArrayBlockHeader)m_blocks[m_readPos];
			}
		}

		if (curBlock->firstId <= id && curBlock->firstId + curBlock->arrayCount > id) {
			// is compress block
			if (m_readPos < m_blocks.size() - 1 && m_blocks[m_readPos + 1] == nullptr) {
				uncompressBlock(m_readPos);
				begin = m_readPos;
				end = begin + COMPRESS_RANGE - 1;
				m_readPos = (begin + end) / 2;
				curBlock = (PArrayBlockHeader)m_blocks[m_readPos];
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
		curBlock = (PArrayBlockHeader)m_blocks[m_readPos];
	}

	if (!isFind) {
		m_readPos = oldReadPos;
	}

	return isFind;
}

uint8_t *ArrayMemoryManager::dataPtr(uint32_t id)
{
	PArrayBlockHeader curBlock = (PArrayBlockHeader)m_blocks[m_readPos];
	if (curBlock->firstId > id || curBlock->firstId + curBlock->arrayCount <= id) {
		return nullptr;
	}

	uint16_t *headerPtr = (uint16_t *)(m_blocks[m_readPos] + sizeof(ArrayBlockHeader));
	uint16_t offset = 0;
	for (uint32_t i = curBlock->firstId; i <= id; ++i) {
		offset += *(headerPtr++);
	}

	return m_blocks[m_readPos] + m_blockByteCount - offset;
}

void ArrayMemoryManager::addReuseBlock(uint32_t blockIndex)
{
	for (int i = 0; i < m_reuseBlocks.size(); ++i) {
		if (m_reuseBlocks[i] == blockIndex) {
			return;
		}
	}

	m_reuseBlocks.push_back(blockIndex);
}

uint32_t ArrayMemoryManager::allocateInReuse(uint32_t len)
{
	uint32_t id = 0;
	for (int i = m_reuseBlocks.size() - 1; i >= 0; --i) {
		uint8_t *block = m_blocks[m_reuseBlocks[i]];
		// compress block
		if (block == nullptr) {
			uncompressBlock(m_reuseBlocks[i]);
			block = m_blocks[m_reuseBlocks[i]];
		}

		PArrayBlockHeader header = (PArrayBlockHeader)block;
		uint16_t avail = m_blockByteCount - sizeof(ArrayBlockHeader) - 
			header->arrayCount * 2 - header->used;
		if (avail >= len) {
			PArrayBlockHeader curBlock = header;
			uint16_t *headerPtr = (uint16_t *)(block + sizeof(ArrayBlockHeader));
			uint16_t insertPos = 0;
			for (int j = 0; j < curBlock->arrayCount; ++j) {
				if (*headerPtr == 0) {
					id = header->firstId + j;
					uint8_t *dataPtr = block + m_blockByteCount - curBlock->used;
					memmove(dataPtr, dataPtr - len, curBlock->used - insertPos);
					curBlock->used += len;
					break;
				}
				insertPos += *(headerPtr++);
			}
			// not exist empty array id, remove from reuseblock
			if (id == 0) {
				m_reuseBlocks.erase(m_reuseBlocks.begin() + i);
			}
		}

		if (id > 0) {
			break;
		}
	}

	return 0;
}

ArrayMemoryManager::ArrayMemoryOperator::ArrayMemoryOperator(ArrayMemoryManager *owner) :
	m_allocator(owner),
	m_data(nullptr)
{
}

ArrayMemoryManager::ArrayMemoryOperator::~ArrayMemoryOperator()
{
}

void ArrayMemoryManager::ArrayMemoryOperator::setID(uint32_t id)
{
	if (m_lastId == id) {
		return;
	}

	m_data = nullptr;
	if (id == ERROR_ID) {
		m_lastId = id;
		return;
	}

	if (m_allocator->moveReadPosToId(id)) {
		m_data = m_allocator->dataPtr(id);
		m_lastId = id;
	}
	else {
		int i = 0;
	}

	if (m_data == nullptr) {
		throw MyException("id is invalid");
	}
}

int8_t ArrayMemoryManager::ArrayMemoryOperator::getBit(uint32_t offset, int8_t index) const
{
	return (m_data[offset] & (1 << index)) == 0 ? 0 : 1;
}

int8_t ArrayMemoryManager::ArrayMemoryOperator::getInt8(uint32_t offset) const
{
	return m_data[offset];
}

int16_t ArrayMemoryManager::ArrayMemoryOperator::getInt16(uint32_t offset) const
{
	int8_t c;
	uint8_t ch1;
	uint8_t ch2;
	c = m_data[offset];
	ch1 = c;
	c = m_data[offset + 1];
	ch2 = c;
	return (((uint16_t)ch1 << 8) + ch2);
}

int32_t ArrayMemoryManager::ArrayMemoryOperator::getInt32(uint32_t offset) const
{
	int8_t c;
	uint8_t ch1;
	uint8_t ch2;
	uint8_t ch3;
	uint8_t ch4;
	c = m_data[offset];
	ch1 = c;
	c = m_data[offset + 1];
	ch2 = c;
	c = m_data[offset + 2];
	ch3 = c;
	c = m_data[offset + 3];
	ch4 = c;
	return (((uint32_t)ch1 << 24) + ((uint32_t)ch2 << 16) + ((uint32_t)ch3 << 8) + ch4);
}

int64_t ArrayMemoryManager::ArrayMemoryOperator::getInt64(uint32_t offset) const
{
	return ((uint64_t)(uint8_t)(m_data[offset]) << 56) +
		((uint64_t)(uint8_t)(m_data[offset + 1] & 255) << 48) +
		((uint64_t)(uint8_t)(m_data[offset + 2] & 255) << 40) +
		((uint64_t)(uint8_t)(m_data[offset + 3] & 255) << 32) +
		((uint64_t)(uint8_t)(m_data[offset + 4] & 255) << 24) +
		((uint64_t)(uint8_t)(m_data[offset + 5] & 255) << 16) +
		((uint64_t)(uint8_t)(m_data[offset + 6] & 255) << 8) +
		((uint64_t)(uint8_t)(m_data[offset + 7] & 255) << 0);
}

uint8_t ArrayMemoryManager::ArrayMemoryOperator::getUint8(uint32_t offset) const
{
	return (uint8_t)getInt8(offset);
}

uint16_t ArrayMemoryManager::ArrayMemoryOperator::getUint16(uint32_t offset) const
{
	return (uint16_t)getInt16(offset);
}

uint32_t ArrayMemoryManager::ArrayMemoryOperator::getUint32(uint32_t offset) const
{
	return (uint32_t)getInt32(offset);
}

uint64_t ArrayMemoryManager::ArrayMemoryOperator::getUint64(uint32_t offset) const
{
	return (uint64_t)getInt64(offset);
}

float ArrayMemoryManager::ArrayMemoryOperator::getFloat32(uint32_t offset) const
{
	float value;
	int32_t *intValue = (int32_t *)&value;
	*intValue = getInt32(offset);
	return value;
}

double ArrayMemoryManager::ArrayMemoryOperator::getFloat64(uint32_t offset) const
{
	double value;
	int64_t *intValue = (int64_t *)&value;
	*intValue = getInt64(offset);
	return value;
}

ByteArray ArrayMemoryManager::ArrayMemoryOperator::getBytes(uint32_t offset, uint32_t len) const
{
	return ByteArray::directFrom(&m_data[offset], len);
}

void ArrayMemoryManager::ArrayMemoryOperator::setBit(uint32_t offset, int8_t index, int8_t value)
{
	if (value > 0) {
		m_data[offset] |= 1 << index;
	}
	else {
		m_data[offset] &= ~(1 << index);
	}
}

void ArrayMemoryManager::ArrayMemoryOperator::setInt8(uint32_t offset, int8_t value)
{
	m_data[offset] = value;
}

void ArrayMemoryManager::ArrayMemoryOperator::setInt16(uint32_t offset, int16_t value)
{
	int8_t tv = (value >> 8) & 0xFF;
	m_data[offset] = tv;
	tv = (value >> 0) & 0xFF;
	m_data[offset + 1] = tv;
}

void ArrayMemoryManager::ArrayMemoryOperator::setInt32(uint32_t offset, int32_t value)
{
	int8_t tv = (value >> 24) & 0xFF;
	m_data[offset] = tv;
	tv = (value >> 16) & 0xFF;
	m_data[offset + 1] = tv;
	tv = (value >> 8) & 0xFF;
	m_data[offset + 2] = tv;
	tv = (value >> 0) & 0xFF;
	m_data[offset + 3] = tv;
}

void ArrayMemoryManager::ArrayMemoryOperator::setInt64(uint32_t offset, int64_t value)
{
	int8_t tv = (value >> 56) & 0xFF;
	m_data[offset] = tv;
	tv = (value >> 48) & 0xFF;
	m_data[offset + 1] = tv;
	tv = (value >> 40) & 0xFF;
	m_data[offset + 2] = tv;
	tv = (value >> 32) & 0xFF;
	m_data[offset + 3] = tv;
	tv = (value >> 24) & 0xFF;
	m_data[offset + 4] = tv;
	tv = (value >> 16) & 0xFF;
	m_data[offset + 5] = tv;
	tv = (value >> 8) & 0xFF;
	m_data[offset + 6] = tv;
	tv = (value >> 0) & 0xFF;
	m_data[offset + 7] = tv;
}

void ArrayMemoryManager::ArrayMemoryOperator::setUint8(uint32_t offset, uint8_t value)
{
	setInt8(offset, (int8_t)value);
}

void ArrayMemoryManager::ArrayMemoryOperator::setUint16(uint32_t offset, uint16_t value)
{
	setInt16(offset, (int16_t)value);
}

void ArrayMemoryManager::ArrayMemoryOperator::setUint32(uint32_t offset, uint32_t value)
{
	setInt32(offset, (int32_t)value);
}

void ArrayMemoryManager::ArrayMemoryOperator::setUint64(uint32_t offset, uint64_t value)
{
	setInt64(offset, (int64_t)value);
}

void ArrayMemoryManager::ArrayMemoryOperator::setFloat32(uint32_t offset, float value)
{
	int32_t *intValue = (int32_t *)(&value);
	setInt32(offset, *intValue);
}

void ArrayMemoryManager::ArrayMemoryOperator::setFloat64(uint32_t offset, double value)
{
	int64_t *intValue = (int64_t *)(&value);
	setInt64(offset, *intValue);
}

void ArrayMemoryManager::ArrayMemoryOperator::setBytes(uint32_t offset, ByteArray bytes)
{
#ifdef _WIN32
	memcpy_s(m_data + offset, bytes->byteLength(), bytes->data(), bytes->byteLength());
#else
	memcpy(m_data + offset, bytes->data(), bytes->byteLength());
#endif
}
