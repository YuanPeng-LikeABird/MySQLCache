#include "InputStream.h"
#ifndef _WIN32
#include <cstring>
#endif

InputStream::InputStream(const ByteArray &buffer) :
	m_data(buffer),
	m_pos(0)
{
}

InputStream::~InputStream()
{
}

bool InputStream::readBoolean()
{
	return readByte() == 1;
}

int8_t InputStream::readByte()
{
	return m_data->getInt8(m_pos++);
}

uint8_t InputStream::readUByte()
{
	return m_data->getUint8(m_pos++);
}

int16_t InputStream::readShort()
{
	int16_t value = m_data->getInt16(m_pos);
	m_pos += 2;
	return value;
}

uint16_t InputStream::readUShort()
{
	uint16_t value = m_data->getUint16(m_pos);
	m_pos += 2;
	return value;
}

int32_t InputStream::readInt()
{
	int32_t value = m_data->getInt32(m_pos);
	m_pos += 4;
	return value;
}

uint32_t InputStream::readUInt()
{
	uint32_t value = m_data->getUint32(m_pos);
	m_pos += 4;
	return value;
}

int64_t InputStream::readLong()
{
	int64_t value = m_data->getInt64(m_pos);;
	m_pos += 8;
	return value;
}

uint64_t InputStream::readULong()
{
	uint64_t value = m_data->getUint64(m_pos);;
	m_pos += 8;
	return value;
}

float InputStream::readFloat()
{
	float value = m_data->getFloat32(m_pos);;
	m_pos += 4;
	return value;
}

double InputStream::readDouble()
{
	double value = m_data->getFloat64(m_pos);;
	m_pos += 8;
	return value;
}

std::string InputStream::readString()
{
	uint16_t length = readUShort();
	if (length > 0) {
		return readCharacters(length);
	}

	return std::string();
}

std::string InputStream::readText()
{
	int32_t length = readInt();
	if (length > 0) {
		return readCharacters(length);
	}
	return std::string();
}

ByteArray InputStream::readBlock()
{
	int32_t length = readInt();

	ByteArray value = m_data->slice(m_pos, m_pos + length);
	m_pos += length;
	return value;
}

MyVariant InputStream::readVariant()
{
	uint8_t t = readUByte();
	if (t == 0) {
		return readDouble();
	}
	else if (t == 1) {
		return readString();
	}
	else if (t == 2) {
		return readBoolean();
	}
}

int InputStream::pos() const
{
	return m_pos;
}

void InputStream::skip(int n)
{
	m_pos += n;
}

bool InputStream::atEnd() const
{
	return m_pos >= m_data->byteLength();
}

std::string InputStream::readCharacters(int length)
{
	std::string value;
	value.resize(length, 0);	
	for (int i = 0; i < length; ++i) {
		value[i] = (char)readUByte();
	}
	return value;
}
