#include "SQLTableSchema.h"
#include "SQLContext.h"
#include "Common.h"
#ifndef _WIN32
#include <cstring>
#endif

FieldSchema::FieldSchema(const std::string &tableName, const std::string &name, DataType type) :
	m_tableName(tableName), 
	m_name(name), 
	m_dataType(type),
	m_isQuery(false),
	m_isPrimary(false)
{
}

FieldSchema::FieldSchema(const FieldSchema &otherField) :
	m_tableName(otherField.tableName()),
	m_name(otherField.name()),
	m_dataType(otherField.dataType()),
	m_isQuery(otherField.isQuery()),
	m_isPrimary(false)
{
}

FieldSchema::~FieldSchema()
{
}

FieldSchema &FieldSchema::operator=(const FieldSchema &otherField)
{
	m_tableName = otherField.tableName();
	m_name = otherField.name();
	m_dataType = otherField.dataType();
	return *this;
}

const std::string &FieldSchema::tableName() const
{
	return m_tableName;
}

const std::string &FieldSchema::name() const
{
	return m_name;
}

DataType FieldSchema::dataType() const
{
	return m_dataType;
}

void FieldSchema::setDataType(DataType type)
{
	m_dataType = type;
}

bool FieldSchema::isQuery() const
{
	return m_isQuery;
}

void FieldSchema::setQuery(bool v)
{
	m_isQuery = v;
}

bool FieldSchema::isPrimaryKey() const
{
	return m_isPrimary;
}

void FieldSchema::setPrimaryKey(bool v)
{
	m_isPrimary = v;
}

SQLNormalTableSchema::SQLNormalTableSchema() :
	SQLTableSchema(),
	m_primaryKey(nullptr)
{
}

SQLNormalTableSchema::~SQLNormalTableSchema()
{
	for (auto i = m_fields.begin(); i != m_fields.end(); ++i) {
		delete *i;
	}
}

void SQLNormalTableSchema::save(WriteBuffer* buffer)
{
	buffer->writeByte((int8_t)kind());
	buffer->writeString(m_name);
	buffer->writeShort(m_fields.size());
	for (int i = 0; i < m_fields.size(); ++i) {
		writeFieldSchema(buffer, *m_fields[i]);
	}
}

TableKind SQLNormalTableSchema::kind()
{
	return TableKind::tkNormal;
}

std::string SQLNormalTableSchema::name() const
{
	return m_name;
}

void SQLNormalTableSchema::setName(const std::string &name)
{
	m_name = name;
}

FieldSchema *SQLNormalTableSchema::addField(FieldSchema &srcFieldSchema)
{
	FieldSchema *field = addField(srcFieldSchema.name(), srcFieldSchema.dataType());
	if (field && srcFieldSchema.isPrimaryKey()) {
		m_primaryKey = field;
		field->setPrimaryKey(true);
	}
	return field;
}

FieldSchema *SQLNormalTableSchema::addField(const std::string &name, DataType type)
{
	FOR_EACH(i, m_fields) {
		if ((*i)->name() == name) {
			return *i;
		}
	}

	FieldSchema *field = new FieldSchema(m_name, name, type);
	m_fields.push_back(field);
	return field;
}

size_t SQLNormalTableSchema::fieldCount() const
{
	return m_fields.size();
}

int SQLNormalTableSchema::fieldIndex(const std::string &name)
{
	auto i = m_fieldIndex.find(name);
	if (i != m_fieldIndex.end()) {
		return i->second;
	}

	return -1;
}

int32_t SQLNormalTableSchema::dataOffSet(const std::string &name)
{
	int index = fieldIndex(name);
	if (index != -1) {
		return m_offsets[index];
	}

	return -1;
}

int32_t SQLNormalTableSchema::recordLength()
{
	if (m_fields.size() == 0) {
		return 0;
	}

	int lastIndex = m_fields.size() - 1;
	return m_offsets[lastIndex] + dataSize(m_fields[lastIndex]->dataType());
}

void SQLNormalTableSchema::copyFieldSchemas(std::vector<FieldSchema *> &dstFields)
{
	dstFields = m_fields;
}

void SQLNormalTableSchema::addColumnMap(const std::string &fieldName, const std::string &colName)
{
	m_sqlColMaps[fieldName] = colName;
}

std::string SQLNormalTableSchema::getRealColumnName(const std::string &fieldName)
{
	auto r = m_sqlColMaps.find(fieldName);
	if (r == m_sqlColMaps.end()) {
		return fieldName;
	}

	return r->second;
}

void SQLNormalTableSchema::compile()
{
	// NullBit is at begin, then is field data by dataType
	uint32_t offset = (m_fields.size() - 1) / 8 + 1;
	for (size_t i = 0; i < m_fields.size(); ++i) {
		m_offsets.push_back(offset);
		FieldSchema *field = m_fields.at(i);
		offset += dataSize(field->dataType());

		m_fieldIndex[field->name()] = i;

		if (field->isPrimaryKey()) {
			m_primaryKey = field;
		}
	}
}

uint32_t SQLNormalTableSchema::dataSize(DataType type)
{
	switch (type)
	{
	case DataType::dtBoolean:
		return 1;
	case DataType::dtSmallInt:
		return 2;
	case DataType::dtInt:
		return 4;
	case DataType::dtBigInt:
		return 8;
	case DataType::dtFloat:
	case DataType::dtDouble:
		return 8;
	case DataType::dtString:
	case DataType::dtBlob:
		return 4;
	default:
		break;
	}

	return 0;
}

void SQLNormalTableSchema::writeFieldSchema(WriteBuffer *buffer, FieldSchema &fieldSchema)
{
	buffer->writeString(fieldSchema.name());
	buffer->writeByte((int8_t)fieldSchema.dataType());
	buffer->writeBoolean(fieldSchema.isQuery());
	buffer->writeBoolean(fieldSchema.isPrimaryKey());
	if (fieldSchema.isQuery()) {
		auto i = m_sqlColMaps.find(fieldSchema.name());
		if (i != m_sqlColMaps.end()) {
			buffer->writeString(i->second);
		}
		else {
			buffer->writeString("");
		}
	}
}

void SQLNormalTableSchema::readFieldSchema(InputStream &in)
{
	std::string name = in.readString();
	DataType dataType = (DataType)in.readByte();
	FieldSchema *field = addField(name, dataType);
	field->setQuery(in.readBoolean());
	field->setPrimaryKey(in.readBoolean());
}

FieldSchema *SQLNormalTableSchema::field(int index) const
{
	return m_fields.at(index);
}

FieldSchema *SQLNormalTableSchema::findField(const std::string &name)
{
	auto iter = m_fieldIndex.find(name);
	if (iter != m_fieldIndex.end()) {
		return m_fields[iter->second];
	}

	return nullptr;
}

FieldSchema *SQLNormalTableSchema::primaryKey() const
{
	return m_primaryKey;
}

SQLExtendTableSchema::SQLExtendTableSchema(SQLNormalTableSchema *base) :
	SQLNormalTableSchema(),
	m_base(base)
{
}

SQLExtendTableSchema::~SQLExtendTableSchema()
{
}

std::string SQLExtendTableSchema::name() const
{
	return m_base->name();
}

TableKind SQLExtendTableSchema::kind()
{
	return TableKind::tkExtend;
}

FieldSchema *SQLExtendTableSchema::addField(const std::string &name, DataType type)
{
	FieldSchema *field = findField(name);
	if (field) {
		return field;
	}

	field = new FieldSchema(m_base->name(), name, type);
	m_fields.push_back(field);
	return field;
}

FieldSchema *SQLExtendTableSchema::field(int index) const
{
	if (index < m_base->fieldCount()) {
		return m_base->field(index);
	}

	return m_fields.at(index - m_base->fieldCount());
}

FieldSchema *SQLExtendTableSchema::findField(const std::string &name)
{
	FieldSchema *result = m_base->findField(name);
	if (!result) {
		auto iter = m_fieldIndex.find(name);
		if (iter != m_fieldIndex.end()) {
			result = m_fields[iter->second];
		}
	}

	return result;
}

FieldSchema *SQLExtendTableSchema::primaryKey() const
{
	return m_base->primaryKey();
}

size_t SQLExtendTableSchema::fieldCount() const
{
	return m_base->fieldCount() + m_fields.size();
}

int SQLExtendTableSchema::fieldIndex(const std::string &name)
{
	int index = m_base->fieldIndex(name);
	if (index >= 0) {
		return index;
	}

	auto i = m_fieldIndex.find(name);
	if (i != m_fieldIndex.end()) {
		return i->second + m_base->fieldCount();
	}

	return -1;
}

int32_t SQLExtendTableSchema::dataOffSet(const std::string &name)
{
	int index = fieldIndex(name);
	return index < m_base->fieldCount() ? m_base->dataOffSet(name) : m_offsets[index - m_base->fieldCount()];
}

int32_t SQLExtendTableSchema::recordLength()
{
	if (m_fields.empty()) {
		return m_base->recordLength();
	}

	int lastIndex = m_fields.size() - 1;
	return m_offsets[lastIndex] + dataSize(m_fields[lastIndex]->dataType());
}

void SQLExtendTableSchema::copyFieldSchemas(std::vector<FieldSchema *> &dstFields)
{
	m_base->copyFieldSchemas(dstFields);
	for (int i = 0; i < m_fields.size(); ++i) {
		dstFields.push_back(m_fields[i]);
	}
}

void SQLExtendTableSchema::compile()
{
	uint32_t offset = m_base->recordLength() + (m_fields.size() - 1) / 8 + 1;
	for (size_t i = 0; i < m_fields.size(); ++i) {
		m_offsets.push_back(offset);
		FieldSchema *field = m_fields.at(i);
		offset += dataSize(field->dataType());

		m_fieldIndex[field->name()] = i;
	}
}

size_t SQLExtendTableSchema::extendFieldCount() const
{
	return m_fields.size();
}

FieldSchema *SQLExtendTableSchema::extendField(int index) const
{
	return m_fields.at(index);
}

int32_t SQLExtendTableSchema::extendOffSet() const
{
	return m_offsets[0] - (m_fields.size() - 1) / 8 - 1;
}

SQLJoinTableSchema::SQLJoinTableSchema() :
	SQLTableSchema(),
	m_join(SQLJoinType::sjtInner)
{
}

SQLJoinTableSchema::~SQLJoinTableSchema()
{
}

std::string SQLJoinTableSchema::name() const
{
	return "";
}

void SQLJoinTableSchema::setName(const std::string &name)
{
}

void SQLJoinTableSchema::save(WriteBuffer* buffer)
{
	buffer->writeByte((int8_t)TableKind::tkJoin);
	m_left.save(buffer);
	m_right.save(buffer);
}

TableKind SQLJoinTableSchema::kind()
{
	return TableKind::tkJoin;
}

void SQLJoinTableSchema::compile()
{
	m_left.compile();
	m_right.compile();
}

int32_t SQLJoinTableSchema::recordLength()
{
	return m_left.recordLength() + m_right.recordLength();
}

void SQLJoinTableSchema::setJoinType(SQLJoinType t)
{
	m_join = t;
}

SQLJoinType SQLJoinTableSchema::joinType() const
{
	return m_join;
}

SQLNormalTableSchema *SQLJoinTableSchema::left()
{
	return &m_left;
}

SQLNormalTableSchema *SQLJoinTableSchema::right()
{
	return &m_right;
}

SQLNormalTableSchema *SQLJoinTableSchema::table(const std::string &tableName)
{
	if (m_left.name() == tableName) {
		return &m_left;
	}
	else if (m_right.name() == tableName) {
		return &m_right;
	}

	return nullptr;
}

SQLTableSchema::SQLTableSchema() :
	m_orderFields(nullptr),
	m_isGroupBy(false)
{
}

SQLTableSchema::~SQLTableSchema()
{
	if (m_orderFields) {
		delete m_orderFields;
	}
}

int SQLTableSchema::orderFieldCount()
{
	if (m_orderFields) {
		return m_orderFields->size();
	}
	
	return 0;
}

const OrderFieldInfo &SQLTableSchema::orderField(int index)
{
	return m_orderFields->at(index);
}

void SQLTableSchema::addOrderField(const OrderFieldInfo &field)
{
	if (!m_orderFields) {
		m_orderFields = new std::vector<OrderFieldInfo>();
	}

	m_orderFields->push_back(field);
}

bool SQLTableSchema::isGroupBy() const
{
	return m_isGroupBy;
}

void SQLTableSchema::setGroupBy(bool value)
{
	m_isGroupBy = value;
}

AggregateFunction functionOf(const std::string &code)
{
	if (strcmp(code.c_str(), "sum") == 0) {
		return AggregateFunction::gfSum;
	}
	else if (strcmp(code.c_str(), "max") == 0) {
		return AggregateFunction::gfMax;
	}
	else if (strcmp(code.c_str(), "min") == 0) {
		return AggregateFunction::gfMin;
	}
	else if (strcmp(code.c_str(), "avg") == 0) {
		return AggregateFunction::gfAvg;
	}
	else if (strcmp(code.c_str(), "count") == 0) {
		return AggregateFunction::gfCount;
	}

	return AggregateFunction();
}

std::string functionStr(AggregateFunction f)
{
	switch (f)
	{
	case AggregateFunction::gfCount:
		return "Count";
	case AggregateFunction::gfSum:
		return "Sum";
	case AggregateFunction::gfAvg:
		return "Avg";
	case AggregateFunction::gfMax:
		return "Max";
	case AggregateFunction::gfMin:
		return "Min";
	default:
		break;
	}

	return "";
}
