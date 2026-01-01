#include "WriteBuffer.h"
#include "MemoryManager.h"
#ifndef _WIN32
#include <cstring>
#endif

WriteBuffer::WriteBuffer(MemoryManager* alloc) :
	m_allocator(alloc), 
	m_data(nullptr),
	m_byteLength(0),
	m_capacity(0),
	m_writePos(0)
{
}

WriteBuffer::~WriteBuffer()
{
	if (m_data) {
		m_allocator->recycle(m_data);
	}
}

uint8_t* WriteBuffer::dataPtr() const
{
	return m_data;
}

uint32_t WriteBuffer::byteLength() const
{
	return m_byteLength;
}

void WriteBuffer::initialize()
{
	m_capacity = m_allocator->writeBufferDefaultMemory();
	m_data = m_allocator->allocate(m_capacity);
}

int32_t WriteBuffer::writePos() const
{
	return m_writePos;
}

void WriteBuffer::seek(int32_t pos)
{
	m_writePos = pos;
}

void WriteBuffer::reset()
{
	m_byteLength = 0;
	m_writePos = 0;
}

void WriteBuffer::writeBoolean(bool value)
{
	prepare(1);
	m_data[m_writePos++] = value ? 1 : 0;
}

void WriteBuffer::writeByte(int8_t value)
{
	prepare(1);
	m_data[m_writePos++] = value;
}

void WriteBuffer::writeUByte(uint8_t value)
{
	writeByte(value);
}

void WriteBuffer::writeShort(int16_t value)
{
	prepare(2);
	int8_t tv = (value >> 8) & 0xFF;
	m_data[m_writePos++] = tv;
	tv = (value >> 0) & 0xFF;
	m_data[m_writePos++] = tv;
}

void WriteBuffer::writeUShort(uint16_t value)
{
	writeShort(value);
}

void WriteBuffer::writeInt(int32_t value)
{
	prepare(4);
	int8_t tv = (value >> 24) & 0xFF;
	m_data[m_writePos++] = tv;
	tv = (value >> 16) & 0xFF;
	m_data[m_writePos++] = tv;
	tv = (value >> 8) & 0xFF;
	m_data[m_writePos++] = tv;
	tv = (value >> 0) & 0xFF;
	m_data[m_writePos++] = tv;
}

void WriteBuffer::writeUInt(uint32_t value)
{
	writeInt(value);
}

void WriteBuffer::writeLong(int64_t value)
{
	prepare(8);
	int8_t tv = (value >> 56) & 0xFF;
	m_data[m_writePos++] = tv;
	tv = (value >> 48) & 0xFF;
	m_data[m_writePos++] = tv;
	tv = (value >> 40) & 0xFF;
	m_data[m_writePos++] = tv;
	tv = (value >> 32) & 0xFF;
	m_data[m_writePos++] = tv;
	tv = (value >> 24) & 0xFF;
	m_data[m_writePos++] = tv;
	tv = (value >> 16) & 0xFF;
	m_data[m_writePos++] = tv;
	tv = (value >> 8) & 0xFF;
	m_data[m_writePos++] = tv;
	tv = (value >> 0) & 0xFF;
	m_data[m_writePos++] = tv;
}

void WriteBuffer::writeULong(uint64_t value)
{
	writeLong(value);
}

void WriteBuffer::writeFloat(float value)
{
	int32_t* intValue = (int32_t*)(&value);
	writeInt(*intValue);
}

void WriteBuffer::writeDouble(double value)
{
	int64_t* intValue = (int64_t*)(&value);
	writeLong(*intValue);
}

void WriteBuffer::writeString(const std::string& value)
{
	uint16_t len = (uint16_t)value.length();
	writeUShort(len);
	writeCharacters(value, len);
}

void WriteBuffer::writeText(const std::string& value)
{
	int32_t len = (int32_t)value.length();
	writeInt(len);
	writeCharacters(value, len);
}

void WriteBuffer::writeCharacters(const std::string& value, int length)
{
	prepare(length);
#ifdef _WIN32
	memcpy_s(m_data + m_writePos, length, value.c_str(), length);
#else
	memcpy(m_data + m_writePos, value.c_str(), length);
#endif
	m_writePos += length;
}

void WriteBuffer::writeBlock(const ByteArray& value)
{
	int len = value->byteLength();
	writeInt(len);
	prepare(len);
#ifdef _WIN32
	memcpy_s(m_data + m_writePos, len, value->data(), len);
#else
	memcpy(m_data + m_writePos, value->data(), len);
#endif
	m_writePos += len;
}

void WriteBuffer::writeBytes(const ByteArray& value)
{
	int len = value->byteLength();
	prepare(len);
#ifdef _WIN32
	memcpy_s(m_data + m_writePos, len, value->data(), len);
#else
	memcpy(m_data + m_writePos, value->data(), len);
#endif
	m_writePos += len;
}

void WriteBuffer::writeVariant(const MyVariant& value)
{
	if (value.isNumber()) {
		writeUByte(0);
		writeDouble(value.toDouble());
	}
	else if (value.isString()) {
		writeUByte(1);
		writeString(value.toString());
	}
	else if (value.type() == MyValueType::mvtBool) {
		writeUByte(2);
		writeBoolean(value.toBool());
	}
}

void WriteBuffer::prepare(int32_t size)
{
	if (m_writePos + size > m_byteLength) {
		m_byteLength = m_writePos + size;
	}

	if (m_capacity >= m_byteLength) {
		return;
	}

	uint32_t oldCapacity = m_capacity;
	while ((m_capacity <<= 1) < m_byteLength)
	{
	}

	uint8_t* oldData = m_data;
	m_data = m_allocator->allocate(m_capacity);
#ifdef _WIN32
	memcpy_s(m_data, oldCapacity, oldData, oldCapacity);
#else
	memcpy(m_data, oldData, oldCapacity);
#endif
	m_allocator->recycle(oldData);
}
