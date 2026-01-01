#include "ByteArray.h"
#ifndef _WIN32
#include <cstring>
#endif

ByteArrayImpl::ByteArrayImpl(uint8_t *data, uint32_t byteLength) :
	m_data(data),
	m_byteLength(byteLength)
{
	m_direct = m_data != nullptr;
}

ByteArrayImpl::~ByteArrayImpl()
{
	if (!m_direct && m_data) {
		free(m_data);
	}	
}

void ByteArrayImpl::assign(uint32_t length)
{
	if (m_data) {
		free(m_data);
	}

	m_data = (uint8_t *)malloc(length);
	m_byteLength = length;
	memset(m_data, 0, length);
}

void ByteArrayImpl::assign(const uint8_t *data, uint32_t length, bool realloc)
{
	if (realloc) {
		if (m_data) {
			free(m_data);
		}

		m_data = (uint8_t *)malloc(length);
		m_byteLength = length;
	}

#ifdef _WIN32
	memcpy_s(m_data, length, data, length);
#else
	memcpy(m_data, data, length);
#endif
}

void ByteArrayImpl::assign(const ByteArray &data, bool realloc)
{
	assign(data->data(), data->byteLength(), realloc);
}

void ByteArrayImpl::assign(const std::string &data)
{
	assign((const uint8_t *)data.c_str(), data.length());
}

void ByteArrayImpl::assign(const ByteArray &other, uint32_t pos)
{
#ifdef _WIN32
	memcpy_s(m_data + pos, other->byteLength(), other->data(), other->byteLength());
#else
	memcpy(m_data + pos, other->data(), other->byteLength());
#endif
}

bool ByteArrayImpl::equal(const ByteArray &buffer) const
{
	if (m_byteLength != buffer->byteLength()) {
		return false;
	}
	
	for (int i = 0; i < m_byteLength; ++i) {
		if (m_data[i] != buffer->data()[i]) {
			return false;
		}
	}

	return true;
}

uint32_t ByteArrayImpl::byteLength() const
{
	return m_byteLength;
}

uint8_t *ByteArrayImpl::data() const
{
	return m_data;
}

ByteArray ByteArrayImpl::slice(uint32_t start, uint32_t end) const
{
	if (end == -1) {
		end = m_byteLength;
	}

	return ByteArray::from(m_data + start, end - start);
}

int8_t ByteArrayImpl::getBit(uint32_t offset, int8_t index) const
{
	return (m_data[offset] & (1 << index)) == 0 ? 0 : 1;
}

int8_t ByteArrayImpl::getInt8(uint32_t offset) const
{
	return m_data[offset];
}

int16_t ByteArrayImpl::getInt16(uint32_t offset) const
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

int32_t ByteArrayImpl::getInt32(uint32_t offset) const
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

int64_t ByteArrayImpl::getInt64(uint32_t offset) const
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

uint8_t ByteArrayImpl::getUint8(uint32_t offset) const
{
	return (uint8_t)getInt8(offset);
}

uint16_t ByteArrayImpl::getUint16(uint32_t offset) const
{
	return (uint16_t)getInt16(offset);
}

uint32_t ByteArrayImpl::getUint32(uint32_t offset) const
{
	return (uint32_t)getInt32(offset);
}

uint64_t ByteArrayImpl::getUint64(uint32_t offset) const
{
	return (uint64_t)getInt64(offset);
}

float ByteArrayImpl::getFloat32(uint32_t offset) const
{
	float value;
	int32_t *intValue = (int32_t *)&value;
	*intValue = getInt32(offset);
	return value;
}

double ByteArrayImpl::getFloat64(uint32_t offset) const
{
	double value;
	int64_t *intValue = (int64_t *)&value;
	*intValue = getInt64(offset);
	return value;
}

void ByteArrayImpl::setBit(uint32_t offset, int8_t index, int8_t value)
{
	if (value > 0) {
		m_data[offset] |= 1 << index;
	}
	else {
		m_data[offset] &= ~(1 << index);
	}
	
}

void ByteArrayImpl::setInt8(uint32_t offset, int8_t value)
{
	m_data[offset] = value;
}

void ByteArrayImpl::setInt16(uint32_t offset, int16_t value)
{
	int8_t tv = (value >> 8) & 0xFF;
	m_data[offset] = tv;
	tv = (value >> 0) & 0xFF;
	m_data[offset + 1] = tv;
}

void ByteArrayImpl::setInt32(uint32_t offset, int32_t value)
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

void ByteArrayImpl::setInt64(uint32_t offset, int64_t value)
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

void ByteArrayImpl::setUint8(uint32_t offset, uint8_t value)
{
	setInt8(offset, (int8_t)value);
}

void ByteArrayImpl::setUint16(uint32_t offset, uint16_t value)
{
	setInt16(offset, (int16_t)value);
}

void ByteArrayImpl::setUint32(uint32_t offset, uint32_t value)
{
	setInt32(offset, (int32_t)value);
}

void ByteArrayImpl::setUint64(uint32_t offset, uint64_t value)
{
	setInt64(offset, (int64_t)value);
}

void ByteArrayImpl::setFloat32(uint32_t offset, float value)
{
	int32_t *intValue = (int32_t *)(&value);
	setInt32(offset, *intValue);
}

void ByteArrayImpl::setFloat64(uint32_t offset, double value)
{
	int64_t *intValue = (int64_t *)(&value);
	setInt64(offset, *intValue);
}
