#include "MyVariant.h"
#include "MathUtils.h"
#include "StrUtils.h"
#include "Common.h"
#include "MyException.h"
#include <limits>

using namespace std;

const std::hash<int32_t> IntHashFunc;
const std::hash<int64_t> LongHashFunc;
const std::hash<double> DoubleHashFunc;
const std::hash<std::string> StringHashFunc;

std::string MyVariant::toString() const
{
	MyValueType type = (MyValueType)m_value.index();
	if (type == MyValueType::mvtString) {
		return std::get<std::string>(m_value);
	}
	else if (type == MyValueType::mvtChar) {
		return std::get<char const*>(m_value);
	}
	else if (type == MyValueType::mvtNull) {
		return "";
	}

	throw MyException("invalid string data");
}

double MyVariant::toDouble() const
{
	MyValueType type = (MyValueType)m_value.index();
	switch (type) {
	case MyValueType::mvtInt8:
		return std::get<int8_t>(m_value);
	case MyValueType::mvtUInt8:
		return std::get<uint8_t>(m_value);
	case MyValueType::mvtInt16:
		return std::get<int16_t>(m_value);
	case MyValueType::mvtUInt16:
		return std::get<uint16_t>(m_value);
	case MyValueType::mvtInt32:
		return std::get<int32_t>(m_value);
	case MyValueType::mvtUInt32:
		return std::get<uint32_t>(m_value);
	case MyValueType::mvtInt64:
		return std::get<int64_t>(m_value);
	case MyValueType::mvtUInt64:
		return std::get<uint64_t>(m_value);
	case MyValueType::mvtDouble:
		return std::get<double>(m_value);
	case MyValueType::mvtFloat:
		return std::get<float>(m_value);
	case MyValueType::mvtString:
		{
			char* endPtr = nullptr;
			string sValue = std::get<string>(m_value);
			double dValue = strtod(sValue.c_str(), &endPtr);
			if (*endPtr == '\0') {
				return dValue;
			}
			else {
				return 0;
			}
		}
		break;
	default:
		throw MyException("invalid double data");
		break;
	}
}

ByteArray MyVariant::toBlob() const
{
	MyValueType type = (MyValueType)m_value.index();
	if (type == MyValueType::mvtBlob) {
		return std::get<ByteArray>(m_value);
	}

	throw MyException("invalid blob data");
}

MyVariant::MyVariant() :
	m_value(nullptr)
{
}

MyVariant::MyVariant(const MyVariant &otherValue) :
	m_value(otherValue.value())
{
}

MyVariant::MyVariant(MyVariant &&otherValue) noexcept :
	m_value(std::move(otherValue.m_value))
{
}

MyVariant::MyVariant(bool value) :
	m_value(value)
{
}

MyVariant::MyVariant(int8_t value) :
	m_value(value)
{
}

MyVariant::MyVariant(uint8_t value) :
	m_value(value)
{
}

MyVariant::MyVariant(int16_t value) :
	m_value(value)
{
}

MyVariant::MyVariant(uint16_t value) :
	m_value(value)
{
}

MyVariant::MyVariant(int32_t value) :
	m_value(value)
{
}

MyVariant::MyVariant(uint32_t value) :
	m_value(value)
{
}

MyVariant::MyVariant(int64_t value) :
	m_value(value)
{
}

MyVariant::MyVariant(uint64_t value) :
	m_value(value)
{
}

MyVariant::MyVariant(float value) :
	m_value(value)
{
}

MyVariant::MyVariant(double value) :
	m_value(value)
{
}

MyVariant::MyVariant(nullptr_t value) :
	m_value(value)
{
}

MyVariant::MyVariant(const std::string& value) :
	m_value(value)
{
}

MyVariant::MyVariant(char const* value) :
	m_value(value)
{
}

MyVariant::MyVariant(ByteArray value) :
	m_value(value)
{
}

MyVariant &MyVariant::operator=(const MyVariant &otherValue)
{
	m_value = otherValue.value();
	return *this;
}

bool MyVariant::operator==(const MyVariant &otherValue) const
{
	return equal(otherValue);
}

bool MyVariant::operator!=(const MyVariant &otherValue) const
{
	return !equal(otherValue);
}

bool MyVariant::operator<(const MyVariant &otherValue) const
{
	return compare(otherValue) < 0;
}

bool MyVariant::operator<=(const MyVariant &otherValue) const
{
	return compare(otherValue) <= 0;
}

bool MyVariant::operator>(const MyVariant &otherValue) const
{
	return compare(otherValue) > 0;
}

bool MyVariant::operator>=(const MyVariant &otherValue) const
{
	return compare(otherValue) >= 0;
}

int8_t MyVariant::toByte() const
{
	MyValueType type = (MyValueType)m_value.index();
	switch (type) {
	case MyValueType::mvtInt8:
		return std::get<int8_t>(m_value);
	case MyValueType::mvtUInt8:
		return std::get<uint8_t>(m_value);
	case MyValueType::mvtInt16:
		return std::get<int16_t>(m_value);
	case MyValueType::mvtUInt16:
		return std::get<uint16_t>(m_value);
	case MyValueType::mvtInt32:
		return std::get<int32_t>(m_value);
	case MyValueType::mvtUInt32:
		return std::get<uint32_t>(m_value);
	case MyValueType::mvtInt64:
		return std::get<int64_t>(m_value);
	case MyValueType::mvtUInt64:
		return std::get<uint64_t>(m_value);
	case MyValueType::mvtDouble:
		return std::get<double>(m_value);
	case MyValueType::mvtFloat:
		return std::get<float>(m_value);
	default:
		throw MyException("invalid int8_t data");
		break;
	}
}

uint8_t MyVariant::toUByte() const
{
	MyValueType type = (MyValueType)m_value.index();
	switch (type) {
	case MyValueType::mvtInt8:
		return std::get<int8_t>(m_value);
	case MyValueType::mvtUInt8:
		return std::get<uint8_t>(m_value);
	case MyValueType::mvtInt16:
		return std::get<int16_t>(m_value);
	case MyValueType::mvtUInt16:
		return std::get<uint16_t>(m_value);
	case MyValueType::mvtInt32:
		return std::get<int32_t>(m_value);
	case MyValueType::mvtUInt32:
		return std::get<uint32_t>(m_value);
	case MyValueType::mvtInt64:
		return std::get<int64_t>(m_value);
	case MyValueType::mvtUInt64:
		return std::get<uint64_t>(m_value);
	case MyValueType::mvtDouble:
		return std::get<double>(m_value);
	case MyValueType::mvtFloat:
		return std::get<float>(m_value);
	default:
		throw MyException("invalid uint8_t data");
		break;
	}
}

bool MyVariant::toBool() const
{
	MyValueType type = (MyValueType)m_value.index();
	if (type == MyValueType::mvtBool) {
		return std::get<bool>(m_value);
	}

	throw MyException("invalid bool data");
}

int16_t MyVariant::toShort() const
{
	MyValueType type = (MyValueType)m_value.index();
	switch (type) {
	case MyValueType::mvtInt8:
		return std::get<int8_t>(m_value);
	case MyValueType::mvtUInt8:
		return std::get<uint8_t>(m_value);
	case MyValueType::mvtInt16:
		return std::get<int16_t>(m_value);
	case MyValueType::mvtUInt16:
		return std::get<uint16_t>(m_value);
	case MyValueType::mvtInt32:
		return std::get<int32_t>(m_value);
	case MyValueType::mvtUInt32:
		return std::get<uint32_t>(m_value);
	case MyValueType::mvtInt64:
		return std::get<int64_t>(m_value);
	case MyValueType::mvtUInt64:
		return std::get<uint64_t>(m_value);
	case MyValueType::mvtDouble:
		return std::get<double>(m_value);
	case MyValueType::mvtFloat:
		return std::get<float>(m_value);
	default:
		throw MyException("invalid short data");
		break;
	}
}

uint16_t MyVariant::toUShort() const
{
	MyValueType type = (MyValueType)m_value.index();
	switch (type) {
	case MyValueType::mvtInt8:
		return std::get<int8_t>(m_value);
	case MyValueType::mvtUInt8:
		return std::get<uint8_t>(m_value);
	case MyValueType::mvtInt16:
		return std::get<int16_t>(m_value);
	case MyValueType::mvtUInt16:
		return std::get<uint16_t>(m_value);
	case MyValueType::mvtInt32:
		return std::get<int32_t>(m_value);
	case MyValueType::mvtUInt32:
		return std::get<uint32_t>(m_value);
	case MyValueType::mvtInt64:
		return std::get<int64_t>(m_value);
	case MyValueType::mvtUInt64:
		return std::get<uint64_t>(m_value);
	case MyValueType::mvtDouble:
		return std::get<double>(m_value);
	case MyValueType::mvtFloat:
		return std::get<float>(m_value);
	default:
		throw MyException("invalid ushort data");
		break;
	}

	throw MyException("invalid ushort data");
}

int32_t MyVariant::toInt() const
{
	MyValueType type = (MyValueType)m_value.index();
	switch (type) {
	case MyValueType::mvtInt8:
		return std::get<int8_t>(m_value);
	case MyValueType::mvtUInt8:
		return std::get<uint8_t>(m_value);
	case MyValueType::mvtInt16:
		return std::get<int16_t>(m_value);
	case MyValueType::mvtUInt16:
		return std::get<uint16_t>(m_value);
	case MyValueType::mvtInt32:
		return std::get<int32_t>(m_value);
	case MyValueType::mvtUInt32:
		return std::get<uint32_t>(m_value);
	case MyValueType::mvtInt64:
		return std::get<int64_t>(m_value);
	case MyValueType::mvtUInt64:
		return std::get<uint64_t>(m_value);
	case MyValueType::mvtDouble:
		return std::get<double>(m_value);
	case MyValueType::mvtFloat:
		return std::get<float>(m_value);
	case MyValueType::mvtString:
		{
			char* endPtr = nullptr;
			string sValue = std::get<string>(m_value);
			int32_t iValue = strtol(sValue.c_str(), &endPtr, 10);
			if (*endPtr == '\0') {
				return iValue;
			}
			else {
				return 0;
			}
		}
		break;
	default:
		throw MyException("invalid int data");
		break;
	}
}

uint32_t MyVariant::toUInt() const
{
	MyValueType type = (MyValueType)m_value.index();
	switch (type) {
	case MyValueType::mvtInt8:
		return std::get<int8_t>(m_value);
	case MyValueType::mvtUInt8:
		return std::get<uint8_t>(m_value);
	case MyValueType::mvtInt16:
		return std::get<int16_t>(m_value);
	case MyValueType::mvtUInt16:
		return std::get<uint16_t>(m_value);
	case MyValueType::mvtInt32:
		return std::get<int32_t>(m_value);
	case MyValueType::mvtUInt32:
		return std::get<uint32_t>(m_value);
	case MyValueType::mvtInt64:
		return std::get<int64_t>(m_value);
	case MyValueType::mvtUInt64:
		return std::get<uint64_t>(m_value);
	case MyValueType::mvtDouble:
		return std::get<double>(m_value);
	case MyValueType::mvtFloat:
		return std::get<float>(m_value);
	case MyValueType::mvtString:
		{
			char* endPtr = nullptr;
			string sValue = std::get<string>(m_value);
			int32_t iValue = strtol(sValue.c_str(), &endPtr, 10);
			if (*endPtr == '\0') {
				return iValue;
			}
			else {
				return 0;
			}
		}
		break;
	default:
		throw MyException("invalid uint data");
		break;
	}
}

int64_t MyVariant::toInt64() const
{
	MyValueType type = (MyValueType)m_value.index();
	switch (type) {
	case MyValueType::mvtInt8:
		return std::get<int8_t>(m_value);
	case MyValueType::mvtUInt8:
		return std::get<uint8_t>(m_value);
	case MyValueType::mvtInt16:
		return std::get<int16_t>(m_value);
	case MyValueType::mvtUInt16:
		return std::get<uint16_t>(m_value);
	case MyValueType::mvtInt32:
		return std::get<int32_t>(m_value);
	case MyValueType::mvtUInt32:
		return std::get<uint32_t>(m_value);
	case MyValueType::mvtInt64:
		return std::get<int64_t>(m_value);
	case MyValueType::mvtUInt64:
		return std::get<uint64_t>(m_value);
	case MyValueType::mvtDouble:
		return std::get<double>(m_value);
	case MyValueType::mvtFloat:
		return std::get<float>(m_value);
	case MyValueType::mvtString:
		{
			char* endPtr = nullptr;
			string sValue = std::get<string>(m_value);
			int64_t iValue = strtoll(sValue.c_str(), &endPtr, 10);
			if (*endPtr == '\0') {
				return iValue;
			}
			else {
				return 0;
			}
		}
		break;
	default:
		throw MyException("invalid int64_t data");
		break;
	}
}

uint64_t MyVariant::toUInt64() const
{
	MyValueType type = (MyValueType)m_value.index();
	switch (type) {
	case MyValueType::mvtInt8:
		return std::get<int8_t>(m_value);
	case MyValueType::mvtUInt8:
		return std::get<uint8_t>(m_value);
	case MyValueType::mvtInt16:
		return std::get<int16_t>(m_value);
	case MyValueType::mvtUInt16:
		return std::get<uint16_t>(m_value);
	case MyValueType::mvtInt32:
		return std::get<int32_t>(m_value);
	case MyValueType::mvtUInt32:
		return std::get<uint32_t>(m_value);
	case MyValueType::mvtInt64:
		return std::get<int64_t>(m_value);
	case MyValueType::mvtUInt64:
		return std::get<uint64_t>(m_value);
	case MyValueType::mvtDouble:
		return std::get<double>(m_value);
	case MyValueType::mvtFloat:
		return std::get<float>(m_value);
	case MyValueType::mvtString:
		{
			char* endPtr = nullptr;
			string sValue = std::get<string>(m_value);
			int64_t iValue = strtoll(sValue.c_str(), &endPtr, 10);
			if (*endPtr == '\0') {
				return iValue;
			}
			else {
				return 0;
			}
		}
		break;
	default:
		throw MyException("invalid uint64_t data");
		break;
	}
}

bool MyVariant::isInt64() const
{
	MyValueType type = (MyValueType)m_value.index();
	return type == MyValueType::mvtUInt64 || type == MyValueType::mvtInt64;
}

bool MyVariant::isInteger() const
{
	MyValueType type = (MyValueType)m_value.index();
	return type >= MyValueType::mvtInt8 && type <= MyValueType::mvtUInt64;
}

bool MyVariant::isNumber() const
{
	MyValueType type = (MyValueType)m_value.index();
	return type >= MyValueType::mvtInt8 && type <= MyValueType::mvtDouble;
}

bool MyVariant::isNull() const
{
	return (MyValueType)m_value.index() == MyValueType::mvtNull;
}

bool MyVariant::isString() const
{
	MyValueType type = (MyValueType)m_value.index();
	return type == MyValueType::mvtString || type == MyValueType::mvtChar;
}

bool MyVariant::equal(const MyVariant &otherValue) const
{
	MyValueType myType = type();
	MyValueType otherType = otherValue.type();
	if (myType == MyValueType::mvtNull || otherType == MyValueType::mvtNull) {
		return myType == otherType;
	}

	if (myType == otherType) {
		if (myType <= MyValueType::mvtInt32) {
			return toInt() == otherValue.toInt();
		}
		else if (myType <= MyValueType::mvtUInt64) {
			return toInt64() == otherValue.toInt64();
		}
		else if (myType <= MyValueType::mvtDouble) {
			return MathUtils::sameFloat(toDouble(), otherValue.toDouble());
		}
		else if (myType <= MyValueType::mvtChar) {
			return toString() == otherValue.toString();
		}
		else if (myType == MyValueType::mvtBool) {
			return toBool() == otherValue.toBool();
		}
		else if (myType == MyValueType::mvtBlob) {
			ByteArray my = toBlob();
			ByteArray other = otherValue.toBlob();
			if (my == nullptr) {
				return other == nullptr || other->byteLength() == 0;
			}
			else {
				return my->equal(other);
			}
		}
	}
	else if (myType <= MyValueType::mvtDouble && otherType <= MyValueType::mvtDouble) {
		if (myType <= MyValueType::mvtUInt64) {
			int64_t myValue = toInt64();
			if (otherType <= MyValueType::mvtUInt64) {
				return myValue == otherValue.toInt64();
			}
			else {
				double otherV = otherValue.toDouble();
				return MathUtils::isInteger(otherV) && (int64_t)otherV == toInt64();
			}
		}
		else {
			double myValue = toDouble();
			if (otherType <= MyValueType::mvtUInt64) {
				return MathUtils::isInteger(myValue) && (int64_t)myValue == otherValue.toInt64();
			}
			else {
				double otherV = otherValue.toDouble();
				return MathUtils::sameFloat(myValue, otherV);
			}
		}
	}
	else if ((myType == MyValueType::mvtString || myType == MyValueType::mvtChar) && 
		(otherType == MyValueType::mvtString || otherType == MyValueType::mvtChar)) {
		return toString() == otherValue.toString();
	}

	return false;
}

int32_t MyVariant::compare(const MyVariant &otherValue) const
{
	MyValueType myType = type();
	MyValueType otherType = otherValue.type();

	if (myType == MyValueType::mvtNull) {
		if (otherType == MyValueType::mvtNull) {
			return 0;
		}
		else {
			return -1;
		}
	}
	else if (otherType == MyValueType::mvtNull) {
		return 1;
	}

	if (myType == otherType) {
		if (myType <= MyValueType::mvtInt32) {
			int32_t my = toInt();
			int32_t other = otherValue.toInt();
			if (my < other) {
				return -1;
			}
			else if (my == other) {
				return 0;
			}
			else {
				return 1;
			}
		}
		else if (myType <= MyValueType::mvtUInt64) {
			int64_t my = toInt64();
			int64_t other = otherValue.toInt64();
			if (my < other) {
				return -1;
			}
			else if (my == other) {
				return 0;
			}
			else {
				return 1;
			}
		}
		else if (myType <= MyValueType::mvtDouble) {
			double my = toDouble();
			double other = otherValue.toDouble();
			if (MathUtils::sameFloat(my, other)) {
				return 0;
			}
			else if (my < other) {
				return -1;
			}
			else {
				return 1;
			}
		}
		else if (myType <= MyValueType::mvtChar) {
			string my = toString();
			string other = otherValue.toString();
			if (my == other) {
				return 0;
			}
			else if (my < other) {
				return -1;
			}
			else {
				return 1;
			}
		}
		else {
			throw MyException("can't compare");
		}
	}
	else if (myType <= MyValueType::mvtDouble && otherType <= MyValueType::mvtDouble) {
		if (myType <= MyValueType::mvtUInt64) {
			int64_t my = toInt64();
			if (otherType <= MyValueType::mvtUInt64) {
				int64_t other = otherValue.toInt64();
				if (my < other) {
					return -1;
				}
				else if (my == other) {
					return 0;
				}
				else {
					return 1;
				}
			}
			else {
				if (my > (int64_t)std::numeric_limits<double>::max()) {
					return 1;
				}
				else if (my < (int64_t)std::numeric_limits<double>::min()) {
					return -1;
				}

				double other = otherValue.toDouble();
				if (MathUtils::sameFloat(my, other)) {
					return 0;
				}
				else if (my < other) {
					return -1;
				}
				else {
					return 1;
				}
			}
		}
		else {
			double my = toDouble();
			if (otherType <= MyValueType::mvtUInt64) {
				int64_t other = otherValue.toInt64();
				if (other > (int64_t)std::numeric_limits<double>::max()) {
					return -1;
				}
				else if (other < (int64_t)std::numeric_limits<double>::min()) {
					return 1;
				}

				if (MathUtils::sameFloat(my, other)) {
					return 0;
				}
				else if (my < other) {
					return -1;
				}
				else {
					return 1;
				}
			}
			else {
				double other = otherValue.toDouble();
				if (MathUtils::sameFloat(my, other)) {
					return 0;
				}
				else if (my < other) {
					return -1;
				}
				else {
					return 1;
				}
			}
		}
	}
	else if ((myType == MyValueType::mvtString || myType == MyValueType::mvtChar) &&
		(otherType == MyValueType::mvtString || otherType == MyValueType::mvtChar)) {
		string my = toString();
		string other = otherValue.toString();
		if (my == other) {
			return 0;
		}
		else if (my < other) {
			return -1;
		}
		else {
			return 1;
		}
	}

	throw MyException("can't compare");
}

const MyVariantImpl& MyVariant::value() const
{
	return m_value;
}

MyValueType MyVariant::type() const
{
	return (MyValueType)m_value.index();
}

size_t MyVariant::hash_value() const
{
	MyValueType type = (MyValueType)m_value.index();
	if (type >= MyValueType::mvtInt8 && type <= MyValueType::mvtUInt64) {
		return LongHashFunc(toInt64());
	}
	else if (type == MyValueType::mvtFloat || type == MyValueType::mvtDouble) {
		return DoubleHashFunc(toDouble());
	}
	else if (type == MyValueType::mvtString || type == MyValueType::mvtChar) {
		return StringHashFunc(toString());
	}

	return 0;
}

std::ostream &operator<<(std::ostream &os, const MyVariant &v)
{
	MyValueType type = v.type();
	switch (type)
	{
	case MyValueType::mvtInt8:
		os << std::get<int8_t>(v.value());
		break;
	case MyValueType::mvtUInt8:
		os << std::get<uint8_t>(v.value());
		break;
	case MyValueType::mvtInt16:
		os << std::get<int16_t>(v.value());
		break;
	case MyValueType::mvtUInt16:
		os << std::get<uint16_t>(v.value());
		break;
	case MyValueType::mvtInt32:
		os << std::get<int32_t>(v.value());
		break;
	case MyValueType::mvtUInt32:
		os << std::get<uint32_t>(v.value());
		break;
	case MyValueType::mvtInt64:
		os << std::get<int64_t>(v.value());
		break;
	case MyValueType::mvtUInt64:
		os << std::get<uint64_t>(v.value());
		break;
	case MyValueType::mvtFloat:
		os << std::get<float>(v.value());
		break;
	case MyValueType::mvtDouble:
		os << std::get<double>(v.value());
		break;
	case MyValueType::mvtChar:
	case MyValueType::mvtString:
		os << v.toString();
		break;
	case MyValueType::mvtBool:
		os << std::get<bool>(v.value());
		break;
	case MyValueType::mvtBlob:
		os << "Blob:" << v.toBlob()->byteLength();
		break;
	default:
		break;
	}
	return os;
}

MyVariants::MyVariants()
{
}

MyVariants::MyVariants(const MyVariants &otherValue)
{
	m_values = otherValue.m_values;
}

MyVariants::MyVariants(MyVariants &&otherValue) noexcept
{
	m_values = otherValue.m_values;
}

MyVariants::~MyVariants()
{
}

bool MyVariants::operator==(const MyVariants &otherValue) const
{
	if (count() != otherValue.count()) {
		return false;
	}

	for (int i = 0; i < count(); ++i) {
		if (variant(i) != otherValue.variant(i)) {
			return false;
		}
	}

	return true;
}

MyVariants &MyVariants::operator=(const MyVariants &otherValue)
{
	m_values = otherValue.m_values;
	return *this;
}

void MyVariants::add(const MyVariant &value)
{
	m_values.push_back(value);
}

void MyVariants::remove(int index)
{
	m_values.erase(m_values.begin() + index);
}

int MyVariants::count() const
{
	return m_values.size();
}

const MyVariant &MyVariants::variant(int index) const
{
	return m_values.at(index);
}

size_t MyVariants::hash_value() const
{
	size_t hashV = 0;
	FOR_EACH(i, m_values) {
		hashV = hashV << 10;
		hashV += (*i).hash_value();
	}
	return hashV;
}
