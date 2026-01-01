#pragma once

#include <iostream>
#include <ios>
#include <string>
#include <cstdint>
#include "ByteArray.h"
#include "MyVariant.h"

class OutputStream
{
public:
	OutputStream();
	~OutputStream();

	ByteArray toByteArray() const;
	
	int32_t pos(bool isAbsolute = true) const;
	void seek(int32_t pos);

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

	void writeString(const std::string &value);
	void writeText(const std::string &value);
	void writeCharacters(const std::string &value, int length);
	void writeBlock(const ByteArray &value);

	void writeVariant(const MyVariant &value);

	void reset();

private:
	void alloc();
	void grow(int32_t len);
	void shrink(int32_t len);
	void updateSize();

private:
	std::vector<uint8_t*> m_blocks;
	int32_t m_prevBlockSize;
	bool m_needShrink;
	int32_t m_curBlockIndex;
	int32_t m_pos;
	// size记录流大小
	int32_t m_size;
};
