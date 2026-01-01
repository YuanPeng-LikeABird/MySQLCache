#pragma once
#include <cstdint>
#include <vector>
#include <string>
#include "ByteArray.h"
#include "MyVariant.h"

class MemoryManager;

class WriteBuffer
{
public:
	WriteBuffer(MemoryManager* alloc);
	~WriteBuffer();

	uint8_t* dataPtr() const;
	uint32_t byteLength() const;

	void initialize();

	int32_t writePos() const;
	void seek(int32_t pos);
	void reset();

	void writeBoolean(bool value);
	void writeByte(int8_t value);
	void writeUByte(uint8_t value);
	void writeShort(int16_t value);
	void writeUShort(uint16_t value);
	void writeInt(int32_t value);
	void writeUInt(uint32_t value);
	void writeLong(int64_t value);
	void writeULong(uint64_t value);
	void writeFloat(float value);
	void writeDouble(double value);

	void writeString(const std::string& value);
	void writeText(const std::string& value);
	void writeCharacters(const std::string& value, int length);
	void writeBlock(const ByteArray& value);
	void writeBytes(const ByteArray& value);

	void writeVariant(const MyVariant& value);

private:
	inline void prepare(int32_t size);

private:
	MemoryManager* m_allocator;
	uint8_t *m_data;
	uint32_t m_capacity;
	uint32_t m_byteLength;
	int32_t m_writePos;
};
