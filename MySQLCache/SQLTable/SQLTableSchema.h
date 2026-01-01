#pragma once

#include "DataType.h"
#include "InputStream.h"
#include "WriteBuffer.h"
#include "Common.h"
#include <string>
#include <vector>
#include <unordered_map>

enum class SQLJoinType
{
	sjtNull = 0,
	sjtInner = 1,
	sjtLef = 2,
	sjtRight = 3
};

enum class TableKind
{
	tkNormal = 0,
	tkJoin = 1,
	tkExtend = 2,
	tkReadOnly = 3
};

enum class OrderType
{
	otAsc = 0,
	otDesc = 1
};

enum class AggregateFunction
{
	gfUnknown = 0,
	gfCount = 1,
	gfSum = 2,
	gfAvg = 3,
	gfMax = 4,
	gfMin = 5
};

AggregateFunction functionOf(const std::string &code);
std::string functionStr(AggregateFunction f);

class FieldSchema;

struct OrderFieldInfo
{
	FieldSchema *field;
	OrderType order;
};

struct AggregateFieldInfo
{
	FieldSchema *field; // 是db的字段，不是查询结果SQLTable的字段
	AggregateFunction aggregateFunc;
	std::string name;
	bool isQuery = false;
};

class SQLTableSchema
{
public:
	SQLTableSchema();
	virtual ~SQLTableSchema();

	virtual std::string name() const = 0;
	virtual void setName(const std::string &name) = 0;

	virtual void save(WriteBuffer *buffer) = 0;

	virtual TableKind kind() = 0;
	virtual void compile() = 0;

	virtual int32_t recordLength() = 0;

	int orderFieldCount();
	const OrderFieldInfo &orderField(int index);
	void addOrderField(const OrderFieldInfo &field);

	bool isGroupBy() const;
	void setGroupBy(bool value);

private:
	std::vector<OrderFieldInfo> *m_orderFields;
	bool m_isGroupBy;
};

class SQLNormalTableSchema : public SQLTableSchema
{
public:
	SQLNormalTableSchema();
	virtual ~SQLNormalTableSchema();

	void save(WriteBuffer *buffer) override;
	TableKind kind() override;

	std::string name() const override;
	void setName(const std::string &name) override;

	int32_t recordLength() override;

	FieldSchema *addField(FieldSchema &srcFieldSchema);
	virtual FieldSchema *addField(const std::string &name, DataType type);
	virtual FieldSchema *field(int index) const;
	virtual FieldSchema *findField(const std::string &name);

	virtual FieldSchema *primaryKey() const;

	virtual size_t fieldCount() const;
	virtual int fieldIndex(const std::string &name);
	virtual int32_t dataOffSet(const std::string &name);

	virtual void copyFieldSchemas(std::vector<FieldSchema *> &dstFields);

	void addColumnMap(const std::string &fieldName, const std::string &colName);
	std::string getRealColumnName(const std::string &fieldName);

	void compile() override;

protected:
	uint32_t dataSize(DataType type);

	void writeFieldSchema(WriteBuffer *buffer, FieldSchema &fieldSchema);
	void readFieldSchema(InputStream &in);

protected:
	FieldSchema *m_primaryKey;
	std::vector<FieldSchema *> m_fields;
	std::vector<uint32_t> m_offsets;
	std::string m_name;
	std::unordered_map<std::string, int32_t> m_fieldIndex;
	std::unordered_map<std::string, std::string> m_sqlColMaps;
};

class SQLExtendTableSchema : public SQLNormalTableSchema
{
public:
	SQLExtendTableSchema(SQLNormalTableSchema *base);
	virtual ~SQLExtendTableSchema();

	std::string name() const override;
	TableKind kind() override;

	FieldSchema *addField(const std::string &name, DataType type) override;
	FieldSchema *field(int index) const override;
	FieldSchema *findField(const std::string &name) override;

	FieldSchema *primaryKey() const override;

	size_t fieldCount() const override;
	int fieldIndex(const std::string &name) override;
	int32_t dataOffSet(const std::string &name) override;

	int32_t recordLength() override;

	void copyFieldSchemas(std::vector<FieldSchema *> &dstFields) override;

	void compile() override;

	size_t extendFieldCount() const;
	FieldSchema *extendField(int index) const;
	int32_t extendOffSet() const;

private:
	SQLNormalTableSchema *m_base;
};

class SQLJoinTableSchema : public SQLTableSchema
{
public:
	SQLJoinTableSchema();
	virtual ~SQLJoinTableSchema();

	std::string name() const override;
	void setName(const std::string &name) override;

	void save(WriteBuffer* buffer) override;

	TableKind kind() override;
	void compile() override;

	int32_t recordLength() override;

	void setJoinType(SQLJoinType t);
	SQLJoinType joinType() const;

	SQLNormalTableSchema *left();
	SQLNormalTableSchema *right();
	SQLNormalTableSchema *table(const std::string &tableName);

private:
	SQLJoinType m_join;
	SQLNormalTableSchema m_left;
	SQLNormalTableSchema m_right;
};

class FieldSchema
{
public:
	FieldSchema(const std::string &tableName, const std::string &name, DataType type = DataType::dtInt);
	FieldSchema(const FieldSchema &otherField);

	~FieldSchema();

	FieldSchema &operator=(const FieldSchema &otherField);

	const std::string &tableName() const;

	const std::string &name() const;

	DataType dataType() const;
	void setDataType(DataType type);

	bool isQuery() const;
	void setQuery(bool v);

	bool isPrimaryKey() const;
	void setPrimaryKey(bool v);

private:
	DataType m_dataType;
	std::string m_name; 
	std::string m_tableName;
	bool m_isQuery;
	bool m_isPrimary;
};
