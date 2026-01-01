#pragma once

#include "SQLTableSchema.h"
#include "SQLGraph.h"
#include "ByteArray.h"
#include "OutputStream.h"
#include "InputStream.h"
#include "WriteBuffer.h"
#include "MyVariant.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <functional>

class SQLRecord;
class SQLJoinRecord;
class SQLNormalRecord;
class SQLTempRecord;

typedef std::unordered_map<int64_t, SQLRecord *> PKHash;
typedef std::unordered_set<SQLJoinRecord *> JOINPKSet;
typedef std::unordered_map<int64_t, JOINPKSet> JOINPKHash;

typedef std::function<void(SQLRecord *)> ForEachRecordEvent;

class SQLTable
{
public:
	SQLTable(SQLTableSchema *tableSchema = nullptr);
	virtual ~SQLTable();
	// 序列化和反序列化，有2个应用场景:1.内存超过阈值则落盘 2.查询结果返回
	// 查询结果和落盘数据一致有一个好处，可以直接把落盘数据作为结果返回
	// isSearch默认为true,为查询结果返回用，为false，表示落盘,落盘结果可以直接当查询结果用，反之不行
	void save(WriteBuffer *buffer);
	ByteArray unload();
	void unload(OutputStream &out);
	void load(ByteArray bytes);
	void load(InputStream &in);

	virtual SQLRecord *newRecord() = 0;
	virtual SQLRecord *append(SQLRecord *rec) = 0;
	virtual int recordCount() const = 0;

	virtual void forEach(const ForEachRecordEvent &e) = 0;

	SQLTableSchema *schema() const;
	void setSchema(SQLTableSchema *schema);

	virtual TableKind kind() const;

	template <typename ForEachCallBack>
	void forEach(const ForEachCallBack &forEachCallBack);

	MyVariant aggregateCalc(FieldSchema *field, std::unordered_set<SQLRecord *> &recs, AggregateFunction f);

	bool compareRecordByOrderFields(SQLRecord *rec1, SQLRecord *rec2);

	virtual void setThreadIndex(int8_t index);
	int8_t threadIndex() const;

	MyVariants &params();
	uint32_t meomoryUsed() const;

protected:
	virtual void doSave(WriteBuffer *buffer);
	virtual void doUnload(OutputStream &out);
	virtual void doLoad(InputStream &in);

protected:
	SQLTableSchema *m_schema;
	MyVariants m_params;
	int8_t m_threadIndex;
	uint32_t m_used;
};

class SQLNormalTable : public SQLTable
{
public:
	SQLNormalTable(SQLTableSchema *schema = nullptr);
	virtual ~SQLNormalTable();

	SQLRecord *newRecord() override;
	SQLRecord *append(SQLRecord *rec) override;
	int recordCount() const override;

	void forEach(const ForEachRecordEvent &e) override;

	FieldSchema *primaryKey() const;
	SQLNormalTableSchema *normalSchema() const;

	std::string name() const;

	// 以下4个函数都是根据主键同步数据,rec不是当前查询结果表中Record对象
	void update(SQLRecord *rec, std::vector<std::string> &updateFieldNames);

	SQLRecord *insert(SQLRecord *rec);

	bool remove(SQLRecord *rec);

	bool removeByPK(int64_t pk);

	bool exist(SQLRecord *rec);
	bool existPK(int64_t pk);

	int64_t intPK(const SQLRecord *rec) const;

	SQLRecord *selectByPK(int64_t pk);

	void doSave(WriteBuffer* buffer) override;
	void doUnload(OutputStream &out) override;
	void doLoad(InputStream &in) override;

protected:
	PKHash m_pkHash;
};

class SQLTempTable : public SQLTable
{
public:
	SQLTempTable(SQLTableSchema* schema = nullptr);
	virtual ~SQLTempTable();

	SQLRecord* newRecord() override;
	SQLRecord* append(SQLRecord* rec) override;
	int recordCount() const override;

	void forEach(const ForEachRecordEvent& e) override;

	FieldSchema* primaryKey() const;
	int64_t intPK(const SQLRecord* rec) const;
	SQLNormalTableSchema* normalSchema() const;

	std::string name() const;

	ByteArrayImpl *data() const;
	void setData(ByteArrayImpl *data);
private:
	std::vector<SQLRecord*> m_recs;
	ByteArrayImpl *m_data;
};

// 只读Table,不允许再修改
class SQLReadOnlyTable : public SQLTable
{
public:
	SQLReadOnlyTable(SQLTableSchema *schema = nullptr);
	virtual ~SQLReadOnlyTable();

	TableKind kind() const override;

	SQLRecord *newRecord() override;
	SQLRecord *append(SQLRecord *rec) override;
	int recordCount() const override;

	void forEach(const ForEachRecordEvent &e) override;

	void doSave(WriteBuffer* buffer) override;
	void doUnload(OutputStream &out) override;
	void doLoad(InputStream &in) override;

private:
	std::vector<SQLNormalRecord *> m_recs;
};

class SQLJoinTable : public SQLTable
{
public:
	SQLJoinTable(SQLTableSchema *tableSchema = nullptr);
	virtual ~SQLJoinTable();

	SQLRecord *newRecord() override;
	SQLRecord *append(SQLRecord *rec) override;
	int recordCount() const override;

	void forEach(const ForEachRecordEvent &e) override;
	void setThreadIndex(int8_t index) override;

	SQLNormalTable &left();
	SQLNormalTable &right();

	SQLJoinTableSchema *joinSchema() const;

	SQLNormalTable *getTable(const std::string &tableName);
	void addJoin(SQLJoinRecord *rec);
	void removeJoin(int64_t pk, bool isLeft);

	// 根据主键同步数据
	bool update(const std::string &tableName, SQLRecord *rec, std::vector<std::string> &updateFields);
	bool remove(const std::string &tableName, SQLRecord *rec);

	void doSave(WriteBuffer* buffer) override;
	void doUnload(OutputStream &out) override;
	void doLoad(InputStream &in) override;

private:
	SQLNormalTable m_leftTable;
	SQLNormalTable m_rightTable;
	// 一种数据结构，表示join记录的对应关系，即left的哪条记录(用ID标识)join上right的哪些记录，是一个多对多的关系
	JOINPKHash m_leftJoinHash;
	JOINPKHash m_rightJoinHash;
};

class SQLRecord
{
public:
	SQLRecord(SQLTable *table);
	virtual ~SQLRecord() {};

	virtual const MyVariant value(const std::string &fieldName) const = 0;
	virtual std::string strValue(const std::string &fieldName) {
		return "";
	}

	virtual void setValue(const std::string &fieldName, const MyVariant &value) = 0;
	virtual int64_t pk() const = 0;

	virtual void save(WriteBuffer *buffer) {};
	virtual void read(std::unordered_map<std::string, MyVariant> &sqlResult,
		bool directColumnName = false) {};

	SQLTable *table() const;

protected:
	SQLTable *m_table;
};

class SQLNormalRecord : public SQLRecord
{
public:
	SQLNormalRecord(SQLTable *table);
	SQLNormalRecord(SQLTable *table, uint32_t dataId);
	virtual ~SQLNormalRecord();

	const MyVariant value(const std::string &fieldName) const override;
	std::string strValue(const std::string &fieldName) override;
	void setValue(const std::string &fieldName, const MyVariant &value) override;
	int64_t pk() const override;

	void save(WriteBuffer* buffer) override;
	void read(std::unordered_map<std::string, MyVariant>& sqlResult,
		bool directColumnName = false) override;

	void writeField(WriteBuffer* buffer, FieldSchema *field, uint32_t offset);

	uint32_t dataId() const;

private:
	void readField(std::unordered_map<std::string, MyVariant>& sqlResult, FieldSchema *field,
		const std::string &sqlFieldName);
	void writeNullBit(WriteBuffer *buffer, SQLNormalTableSchema *tableSchema);
	void setNull(int fieldIndex, bool value);
	bool isNull(int fieldIndex) const;
private:
	uint32_t m_dataId;
};

class SQLTempRecord : public SQLRecord
{
public:
	SQLTempRecord(SQLTable* table);
	virtual ~SQLTempRecord();

	const MyVariant value(const std::string& fieldName) const override;
	std::string strValue(const std::string& fieldName) override;
	void setValue(const std::string& fieldName, const MyVariant& value) override;
	int64_t pk() const override;

	void load(InputStream &in);

private:
	bool isNull(int fieldIndex) const;
private:
	int32_t *m_offsets;
};

class SQLJoinRecord : public SQLRecord
{
public:
	SQLJoinRecord(SQLTable *table, SQLRecord *left = nullptr, SQLRecord *right = nullptr);
	virtual ~SQLJoinRecord();

	const MyVariant value(const std::string &fieldName) const override;
	std::string strValue(const std::string &fieldName) override;
	void setValue(const std::string &fieldName, const MyVariant &value) override;
	int64_t pk() const override;

	void save(WriteBuffer* buffer) override;
	void read(std::unordered_map<std::string, MyVariant>& sqlResult,
		bool directColumnName = false) override;

	SQLRecord *left() const;
	SQLRecord *right() const;

	SQLJoinTable *joinTable() const;
	void add(SQLRecord *rec);

private:
	SQLRecord *m_left;
	SQLRecord *m_right;
};

class SQLExtendRecord : public SQLRecord
{
public:
	SQLExtendRecord(SQLRecord *base = nullptr);
	~SQLExtendRecord();

	void setBase(SQLRecord *base);

	void addMapFields(const std::string &srcFieldName, const std::string &dstFieldName);

	const MyVariant value(const std::string &fieldName) const override;
	void setValue(const std::string &fieldName, const MyVariant &value) override;
	int64_t pk() const override;

private:
	SQLRecord *m_base;
	std::unordered_map<std::string, std::string> m_fieldMap;
};

template<typename ForEachCallBack>
inline void SQLTable::forEach(const ForEachCallBack &forEachCallBack)
{
	const ForEachRecordEvent &e = forEachCallBack;
	forEach(e);
}

