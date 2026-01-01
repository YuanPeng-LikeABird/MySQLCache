#pragma once

#include <memory>
#include <string>

class ByteArray;

class ByteArrayImpl
{
public:
	explicit ByteArrayImpl(uint8_t *data = nullptr, uint32_t byteLength = 0);
	~ByteArrayImpl();

	void assign(uint32_t length);
	void assign(const uint8_t *data, uint32_t length, bool realloc = true);
	void assign(const ByteArray &data, bool realloc = true);
	void assign(const std::string &data);
	void assign(const ByteArray &other, uint32_t pos);

	bool equal(const ByteArray &buffer) const;

	uint32_t byteLength() const;
	uint8_t *data() const;
	ByteArray slice(uint32_t start, uint32_t end = -1) const;

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

private:
	uint8_t *m_data;
	uint32_t m_byteLength;
	bool m_direct;
};

class ByteArray : public std::shared_ptr<ByteArrayImpl> {
public:
	explicit ByteArray(ByteArrayImpl *impl = nullptr) :
		std::shared_ptr<ByteArrayImpl>(impl) {
	}

	static ByteArray from(uint32_t length) {
        ByteArray buffer(new ByteArrayImpl());
        buffer->assign(length);
        return buffer;
	}

	static ByteArray from(uint8_t *data, uint32_t length) {
        ByteArray buffer(new ByteArrayImpl());
        buffer->assign(data, length);
        return buffer;
	}

	static ByteArray directFrom(uint8_t *data, uint32_t length) {
		ByteArray buffer(new ByteArrayImpl(data, length));
		return buffer;
	}

	static ByteArray from(ByteArray data) {
        ByteArray buffer(new ByteArrayImpl());
        buffer->assign(data);
        return buffer;
	}

	static ByteArray from(ByteArray data, uint32_t length) {
		ByteArray buffer(new ByteArrayImpl());
		buffer->assign(length);
		buffer->assign(data, false);
		return buffer;
	}

	static ByteArray from(const std::string &data) {
        ByteArray buffer(new ByteArrayImpl());
        buffer->assign(data);
        return buffer;
	}	
};

