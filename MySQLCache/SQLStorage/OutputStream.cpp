#include "OutputStream.h"
#include "MyException.h"
#include "MathUtils.h"
#ifndef _WIN32
#include <cstring>
#endif

using namespace std;

const int32_t BLOCK_CAPACITY = 1024;

OutputStream::OutputStream() :
	m_prevBlockSize(0),
	m_needShrink(false),
	m_curBlockIndex(-1),
	m_pos(0),
	m_size(0)
{
	alloc();
}

OutputStream::~OutputStream()
{
	for (int i = 0; i < m_blocks.size(); ++i) {
		free(m_blocks[i]);
	}
}

ByteArray OutputStream::toByteArray() const
{
	ByteArray buffer = ByteArray::from(m_size);
	uint8_t* data = const_cast<uint8_t*>(buffer->data());
	int32_t pos = 0;
	for (int i = 0; i < m_blocks.size() - 1; ++i) {
		memcpy(data + pos, m_blocks[i], BLOCK_CAPACITY);
		pos += BLOCK_CAPACITY;
	}
	memcpy(data + pos, m_blocks[m_blocks.size() - 1], m_size - pos);

	return buffer;
}

int32_t OutputStream::pos(bool isAbsolute) const
{
	if (isAbsolute) {
		return (m_blocks.size() - 1) * BLOCK_CAPACITY + m_pos;
	}

	return m_pos;
}

void OutputStream::seek(int32_t pos)
{
	if (pos > m_size) {
		throw MyException(L"seek out of bounds");
	}

	int32_t blockIndex = pos / BLOCK_CAPACITY;
	if (pos == BLOCK_CAPACITY * blockIndex) {
		m_curBlockIndex = blockIndex - 1;
		if (m_curBlockIndex < 0) {
			m_curBlockIndex = 0;
		}
	}
	else {
		m_curBlockIndex = blockIndex;
	}

	m_pos = pos - m_curBlockIndex * BLOCK_CAPACITY;
}

void OutputStream::writeBoolean(bool value)
{
	grow(1);
	if (value) {
		m_blocks[m_curBlockIndex][m_pos] = 1;
	}
	else {
		m_blocks[m_curBlockIndex][m_pos] = 0;
	}

	if (m_needShrink) {
		shrink(1);
	}
	else {
		m_pos += 1;
	}

	updateSize();
}

void OutputStream::writeByte(int8_t value)
{
	grow(1);
	m_blocks[m_curBlockIndex][m_pos] = value;

	if (m_needShrink) {
		shrink(1);
	}
	else {
		m_pos += 1;
	}

	updateSize();
}

void OutputStream::writeUByte(uint8_t value)
{
	writeByte((int8_t)value);
}

void OutputStream::writeShort(int16_t value)
{
	grow(2);
	int8_t tv = (value >> 8) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos] = tv;
	tv = (value >> 0) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos + 1] = tv;
	if (m_needShrink) {
		shrink(2);
	}
	else {
		m_pos += 2;
	}

	updateSize();
}

void OutputStream::writeUShort(uint16_t value)
{
	writeShort((int16_t)value);
}

void OutputStream::writeInt(int32_t value)
{
	grow(4);
	int8_t tv = (value >> 24) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos] = tv;
	tv = (value >> 16) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos + 1] = tv;
	tv = (value >> 8) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos + 2] = tv;
	tv = value & 0xFF;
	m_blocks[m_curBlockIndex][m_pos + 3] = tv;
	if (m_needShrink) {
		shrink(4);
	}
	else {
		m_pos += 4;
	}

	updateSize();
}

void OutputStream::writeUInt(uint32_t value)
{
	writeInt((int32_t)value);
}

void OutputStream::writeLong(int64_t value)
{
	grow(8);
	int8_t tv = (value >> 56) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos] = tv;
	tv = (value >> 48) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos + 1] = tv;
	tv = (value >> 40) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos + 2] = tv;
	tv = (value >> 32) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos + 3] = tv;
	tv = (value >> 24) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos + 4] = tv;
	tv = (value >> 16) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos + 5] = tv;
	tv = (value >> 8) & 0xFF;
	m_blocks[m_curBlockIndex][m_pos + 6] = tv;
	tv = value & 0xFF;
	m_blocks[m_curBlockIndex][m_pos + 7] = tv;
	if (m_needShrink) {
		shrink(8);
	}
	else {
		m_pos += 8;
	}

	updateSize();
}

void OutputStream::writeULong(uint64_t value)
{
	writeLong((int64_t)value);
}

void OutputStream::writeFloat(float value)
{
	int32_t *intValue = (int32_t *)(&value);
	writeInt(*intValue);
}

void OutputStream::writeDouble(double value)
{
	int64_t *intValue = (int64_t *)(&value);
	writeLong(*intValue);
}

void OutputStream::writeString(const std::string &value)
{
	uint16_t len = (uint16_t)value.length();
	writeUShort(len);
	writeCharacters(value, len);
}

void OutputStream::writeText(const std::string &value)
{
	int32_t len = (int32_t)value.length();
	writeInt(len);
	writeCharacters(value, len);
}

void OutputStream::writeBlock(const ByteArray &value)
{
	int length = value->byteLength();
	writeInt(length);
	for (uint32_t i = 0; i < length; ++i) {
		writeUByte(value->getUint8(i));
	}
}

void OutputStream::writeVariant(const MyVariant &value)
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

void OutputStream::reset()
{
	m_size = 0;
	m_curBlockIndex = 0;
}

void OutputStream::alloc()
{
	++m_curBlockIndex;
	if (m_curBlockIndex >= m_blocks.size()) {
		m_blocks.push_back((uint8_t*)malloc(BLOCK_CAPACITY));

	}
	m_pos = 0;
}

void OutputStream::grow(int32_t len)
{
	m_needShrink = false;
	if (m_pos + len > BLOCK_CAPACITY) {
		m_prevBlockSize = m_pos;
		if (m_prevBlockSize < BLOCK_CAPACITY) {
			m_needShrink = true;
		}
		alloc();
	}
}

void OutputStream::shrink(int32_t len)
{
	if (!m_needShrink) {
		return;
	}
	// 上一个buffer剩余字节数
	int32_t moveBytes = BLOCK_CAPACITY - m_prevBlockSize;
	uint8_t* prevData = m_blocks[m_curBlockIndex - 1];
	uint8_t* curData = m_blocks[m_curBlockIndex];

	memcpy(prevData + m_prevBlockSize, curData, moveBytes);
	memcpy(curData, curData + moveBytes, len - moveBytes);
	m_pos = len - moveBytes;
	m_needShrink = false;
}

void OutputStream::updateSize()
{
	int32_t newSize = pos();
	if (newSize > m_size) {
		m_size = newSize;
	}
}

void OutputStream::writeCharacters(const std::string &value, int length)
{
	for (int i = 0; i < length; ++i) {
		writeUByte((uint8_t)value[i]);
	}
}
