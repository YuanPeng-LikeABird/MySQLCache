#pragma once

#include "ByteArray.h"
#include <string>
#include <variant>
#include <cstdint>
#include <iostream>
#include <vector>

typedef std::variant<int8_t, uint8_t, int16_t, uint16_t, int32_t, uint32_t, int64_t, uint64_t, float, double, std::string, char const*, bool, ByteArray, nullptr_t> MyVariantImpl;

enum class MyValueType
{
	mvtInt8 = 0,
	mvtUInt8,
	mvtInt16,
	mvtUInt16,
	mvtInt32,
	mvtUInt32,
	mvtInt64,
	mvtUInt64,
	mvtFloat,
	mvtDouble,
	mvtString,
	mvtChar,
	mvtBool,
	mvtBlob,
	mvtNull
};

class MyVariant
{
public:
	MyVariant();
	MyVariant(const MyVariant &otherValue);
	MyVariant(MyVariant &&otherValue) noexcept;

	MyVariant(bool value);
	MyVariant(int8_t value);
	MyVariant(uint8_t value);
	MyVariant(int16_t value);
	MyVariant(uint16_t value);
	MyVariant(int32_t value);
	MyVariant(uint32_t value);
	MyVariant(int64_t value);
	MyVariant(uint64_t value);
	MyVariant(float value);
	MyVariant(double value);
	MyVariant(nullptr_t value);
	MyVariant(const std::string& value);
	MyVariant(char const* value);
	MyVariant(ByteArray value);

	MyVariant &operator=(const MyVariant &otherValue);
	bool operator==(const MyVariant &otherValue) const;
	bool operator!=(const MyVariant &otherValue) const;

	bool operator<(const MyVariant &otherValue) const;
	bool operator<=(const MyVariant &otherValue) const;
	bool operator>(const MyVariant &otherValue) const;
	bool operator>=(const MyVariant &otherValue) const;

	int8_t toByte() const;
	uint8_t toUByte() const;
	bool toBool() const;
	int16_t toShort() const;
	uint16_t toUShort() const;
	int32_t toInt() const;
	uint32_t toUInt() const;
	int64_t toInt64() const;
	uint64_t toUInt64() const;
	std::string toString() const;
	double toDouble() const;
	ByteArray toBlob() const;

	bool isInt64() const;
	bool isInteger() const;
	bool isNumber() const;
	bool isNull() const;
	bool isString() const;

	const MyVariantImpl& value() const;
	MyValueType type() const;

	size_t hash_value() const;

private:
	bool equal(const MyVariant &otherValue) const;
	int32_t compare(const MyVariant &otherValue) const;

private:
	MyVariantImpl m_value;
};

class MyVariants {
public:
	MyVariants();
	MyVariants(const MyVariants &otherValue);
	MyVariants(MyVariants &&otherValue) noexcept;

	~MyVariants();

	bool operator==(const MyVariants &otherValue) const;
	MyVariants &operator=(const MyVariants &otherValue);

	void add(const MyVariant &value);
	void remove(int index);

	int count() const;
	const MyVariant &variant(int index) const;

	size_t hash_value() const;

private:
	std::vector<MyVariant> m_values;
};

std::ostream &operator<<(std::ostream &os, const MyVariant &v);

namespace std
{
#ifdef _WIN32
	template <>
	struct hash<MyVariant> {
		_CXX17_DEPRECATE_ADAPTOR_TYPEDEFS typedef MyVariant argument_type;
		_CXX17_DEPRECATE_ADAPTOR_TYPEDEFS typedef size_t result_type;
		_NODISCARD size_t operator()(const MyVariant &_Keyval) const noexcept {
			return _Keyval.hash_value();
		}
	};

	template <>
	struct hash<MyVariants> {
		_CXX17_DEPRECATE_ADAPTOR_TYPEDEFS typedef MyVariants argument_type;
		_CXX17_DEPRECATE_ADAPTOR_TYPEDEFS typedef size_t result_type;
		_NODISCARD size_t operator()(const MyVariants &_Keyval) const noexcept {
			return _Keyval.hash_value();
		}
	};
#else
	template <>
	struct hash<MyVariant> {
		typedef MyVariant argument_type;
		typedef size_t result_type;
		size_t operator()(const MyVariant &_Keyval) const noexcept {
			return _Keyval.hash_value();
		}
	};

	template <>
	struct hash<MyVariants> {
		typedef MyVariants argument_type;
		typedef size_t result_type;
		size_t operator()(const MyVariants &_Keyval) const noexcept {
			return _Keyval.hash_value();
		}
	};
#endif
	
}