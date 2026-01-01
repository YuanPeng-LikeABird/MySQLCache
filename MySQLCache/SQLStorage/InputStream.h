#pragma once

#include <iostream>
#include <string>
#include <cstdint>
#include "ByteArray.h"
#include "MyVariant.h"

class InputStream
{
public:
	explicit InputStream(const ByteArray &buffer);
	~InputStream();

	bool readBoolean();
	int8_t readByte();
	uint8_t readUByte();
	int16_t readShort();
	uint16_t readUShort();
	int32_t readInt();
	uint32_t readUInt();
	int64_t readLong();
	uint64_t readULong();
	float readFloat();
	double readDouble();

	std::string readString();
	std::string readText();
	std::string readCharacters(int length);

	ByteArray readBlock();
	MyVariant readVariant();

	int pos() const;
	void skip(int n);

	bool atEnd() const;

private:
	int m_pos;
	ByteArray m_data;
};

