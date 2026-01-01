#pragma once
#include <cstdint>
#include <vector>
#include <unordered_map>
#include <string>
#include "SwapFile.h"
#include "Task.h"

class MemoryManager;

class ArrayMemoryManager
{
public:
	class ArrayMemoryOperator
	{
	public:
		ArrayMemoryOperator(ArrayMemoryManager *owner);
		~ArrayMemoryOperator();

		void setID(uint32_t id);

		int8_t getBit(uint32_t offset, int8_t index) const;
		int8_t getInt8(uint32_t offset) const;
		int16_t getInt16(uint32_t offset) const;
		int32_t getInt32(uint32_t offset) const;
		int64_t getInt64(uint32_t offset) const;
		uint8_t getUint8(uint32_t offset) const;
		uint16_t getUint16(uint32_t offset) const;
		uint32_t getUint32(uint32_t offset) const;
		uint64_t getUint64(uint32_t offset) const;
		float getFloat32(uint32_t offset) const;
		double getFloat64(uint32_t offset) const;
		ByteArray getBytes(uint32_t offset, uint32_t len) const;

		void setBit(uint32_t offset, int8_t index, int8_t value);
		void setInt8(uint32_t offset, int8_t value);
		void setInt16(uint32_t offset, int16_t value);
		void setInt32(uint32_t offset, int32_t value);
		void setInt64(uint32_t offset, int64_t value);
		void setUint8(uint32_t offset, uint8_t value);
		void setUint16(uint32_t offset, uint16_t value);
		void setUint32(uint32_t offset, uint32_t value);
		void setUint64(uint32_t offset, uint64_t value);
		void setFloat32(uint32_t offset, float value);
		void setFloat64(uint32_t offset, double value);
		void setBytes(uint32_t offset, ByteArray bytes);

	private:
		ArrayMemoryManager *m_allocator;
		uint8_t *m_data;
		uint32_t m_lastId;
	};

	ArrayMemoryManager(MemoryManager *owner);
	~ArrayMemoryManager();

	void initialize();
	void reset();
	void pushBlock(PushBlockTaskData *data);

	uint32_t allocate(uint32_t len);
	void recycle(uint32_t id);
	std::string used() const;

	ArrayMemoryOperator &memoryOperator(uint32_t id = 0);

private:
	void assureNewBlock();
	void compressBlock();
	void uncompressBlock(uint32_t index);

	void preparePushBlock();

	void pullBlock(uint32_t index);

	bool moveReadPosToId(uint32_t id);
	uint8_t *dataPtr(uint32_t id);
	void addReuseBlock(uint32_t blockIndex);
	uint32_t allocateInReuse(uint32_t len);

private:
	uint32_t m_blockByteCount;

	MemoryManager *m_allocator;
	ArrayMemoryOperator m_operator;
	SwapFile *m_swap;

	std::vector<uint8_t *> m_blocks;
	std::unordered_map<uint32_t, uint8_t *> m_bigArrays;
	// read/write cursor,read for get, write for set, while read cursor pos is random, write curpor is always backward
	uint32_t m_readPos;
	uint32_t m_writePos;
	uint8_t *m_headCursor;
	uint8_t *m_dataCursor;
	uint32_t m_maxId;
	uint32_t m_maxBigId;
	std::vector<uint32_t> m_reuseBlocks;
	uint64_t m_used;
	bool m_inPush;
};

