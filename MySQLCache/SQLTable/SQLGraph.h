#pragma once

#include "SQLTableSchema.h"
#include "SQLTableIndex.h"
#include <memory>

class SimpleCondition;
class Condition;
class ConstCondition;
class SQLTable;
class SQLEdge;
class SQLRecord;

enum class ConditionKind
{
	ckConst = 0,
	ckField = 1,
	ckBinary = 2,
	ckAggregateConst = 3,
	ckAggregateField = 4
};

enum class VertexType
{
	vtUnknown = 0,
	vtSchema = 1,
	vtField = 2
};

class SQLVertex
{
public:
	SQLVertex();
	virtual ~SQLVertex();

	virtual VertexType type() = 0;
};

class SQLFieldVertex : public SQLVertex
{
public:
	SQLFieldVertex(FieldSchema *schema = nullptr);
	virtual ~SQLFieldVertex();

	VertexType type() override;

	FieldSchema *fieldSchema() const;
	void setFieldSchema(FieldSchema *schema);

	SQLEdge *appendEdge(SQLVertex *vtx);
	int edgeCount() const;
	SQLEdge *edge(int index);
private:
	std::vector<SQLEdge *> m_edges;
	FieldSchema *m_fieldSchema;
};

class SQLSchemaVertex : public SQLVertex
{
public:
	SQLSchemaVertex(SQLTableSchema *schema = nullptr);
	virtual ~SQLSchemaVertex();

	VertexType type() override;

	SQLTableSchema *schema() const;
	void setSchema(SQLTableSchema *schema);

	std::shared_ptr<Condition> condition() const;
	void setCondition(std::shared_ptr<Condition> c);

	void addTable(SQLTable *table, uint32_t tableId, int thIndex);
	void clearTable(int thIndex);

	int tableCount() const;
	std::vector<SQLTable *> findTable(SQLRecord *rec, int thIndex);
	SQLTable *findTable(const MyVariants &key, int thIndex);

private:
	void buildIndexCondition();
	void innerBuildIndexCondition(Condition *cond);
	std::vector<uint32_t> findTableByIndex(SQLRecord *rec);
	void addTableToIndex(SQLTable *table, uint32_t tableId, int thIndex);

private:
	SQLTableSchema *m_schema;
	std::shared_ptr<Condition> m_where;
	ConstCondition *m_indexCondition;
	SQLTableIndex *m_index;
	std::unordered_map<MyVariants, uint32_t> m_tables;
};

class RelationUtils
{
public:
	static void setQuery(uint8_t &relation, bool v);
	static bool isQuery(uint8_t relation);

	static void setWhere(uint8_t &relation, bool v);
	static bool isWhere(uint8_t relation);

	static void setOrder(uint8_t &relation, bool v);
	static bool isOrder(uint8_t relation);

	static void set(uint8_t &dstRelation, uint8_t srcRelation);

private:
	static void set(uint8_t &relation, int bitIndex, bool v);
	static bool get(uint8_t relation, int bitIndex);
};

class SQLEdge
{
public:
	SQLEdge();
	~SQLEdge();

	void setQuery(bool v);
	bool isQuery() const;

	void setWhere(bool v);
	bool isWhere() const;

	void setOrder(bool v);
	bool isOrder() const;

	uint8_t relations() const;

	SQLVertex *to() const;
	void setTo(SQLVertex *to);

private:
	SQLVertex *m_to;
	uint8_t m_relations;
};

class SQLGraph
{
	typedef std::unordered_map<intptr_t, SQLVertex *> VertexHash;

public:
	SQLGraph(int threadIndex);
	~SQLGraph();

	SQLVertex *addVertex(FieldSchema *fieldSchema);
	SQLVertex *addVertex(SQLTableSchema *tableSchema);

	void freeVertex(SQLVertex *vtx);

	SQLVertex *findVertex(intptr_t schema);
private:
	int m_threadIndex;
	VertexHash m_vtxs;
};

class CompareOperator
{
public:
	virtual ~CompareOperator() {}
};

class BinaryCompareOperator : public CompareOperator
{
public:
	virtual bool compare(const MyVariant &v1, const MyVariant &v2) = 0;
	virtual bool compare(const MyVariant& v1, const MyVariants& v2) 
	{ 
		return false; 
	}

	static BinaryCompareOperator *buildOperator(const std::string &op);
};

class GreaterOperator : public BinaryCompareOperator
{
public:
	GreaterOperator();
	virtual ~GreaterOperator();

	bool compare(const MyVariant &v1, const MyVariant &v2) override;

	bool includeEqual() const;
	void setIncludeEqual(bool e);
private:
	bool m_includeEqual;
};

class LesserOperator : public BinaryCompareOperator
{
public:
	LesserOperator();
	virtual ~LesserOperator();

	bool compare(const MyVariant &v1, const MyVariant &v2) override;

	bool includeEqual() const;
	void setIncludeEqual(bool e);
private:
	bool m_includeEqual;
};

class EqualOperator : public BinaryCompareOperator
{
public:
	bool compare(const MyVariant &v1, const MyVariant &v2) override;
};

class UnEqualOperator : public BinaryCompareOperator
{
public:
	bool compare(const MyVariant &v1, const MyVariant &v2) override;
};

class BetweenOperator : public BinaryCompareOperator
{
public:
	bool compare(const MyVariant& v1, const MyVariant& v2) override;
	bool compare(const MyVariant &v1, const MyVariants &v2) override;
};

class InOperator : public BinaryCompareOperator
{
public:
	bool compare(const MyVariant& v1, const MyVariant &v2) override;
	bool compare(const MyVariant &v1, const MyVariants &v2) override;
};

class UnaryCompareOperator : public CompareOperator
{
public:
	virtual bool compare(const MyVariant &v1) = 0;
};

class Condition
{
public:
	Condition(const std::string &op);
	~Condition();

	virtual ConditionKind kind() = 0;

	const std::string &op() const;
	void setOp(const std::string &op);

	virtual bool match(SQLRecord *rec, MyVariants &params) = 0;
	virtual void toString(std::string &result, SQLRecord *joinTableRec, 
		std::vector<int16_t> &paramIndex) = 0;
private:
	std::string m_op;
	std::string m_expr;
};

class BinaryCondition : public Condition
{
public:
	BinaryCondition(const std::string &op);
	virtual ~BinaryCondition();

	ConditionKind kind() override;
	bool match(SQLRecord *rec, MyVariants &params) override;
	void toString(std::string &result, SQLRecord *joinTableRec, 
		std::vector<int16_t>& paramIndex) override;

	Condition *left();
	void setLeft(Condition *condition);

	Condition *right();
	void setRight(Condition *condition);

private:
	Condition *m_left;
	Condition *m_right;
};

class SimpleCondition : public Condition
{
public:
	SimpleCondition(const std::string &op);
	virtual ~SimpleCondition();
};

class ConstCondition : public SimpleCondition
{
public:
	ConstCondition(FieldSchema *leftField, int32_t startParamId, int32_t endParamId, 
		const std::string &op);
	virtual ~ConstCondition();

	int32_t startParamID() const;
	int32_t endParamID() const;

	ConditionKind kind() override;
	bool match(SQLRecord *rec, MyVariants &params) override;
	void toString(std::string &result, SQLRecord *joinTableRec, 
		std::vector<int16_t>& paramIndex) override;

	FieldSchema *leftField() const;

protected:
	FieldSchema *m_leftField;
	int16_t m_startParamId;
	int16_t m_endParamId;
	BinaryCompareOperator *m_comparator;
};

class AggregateConstCondition : public ConstCondition
{
public:
	AggregateConstCondition(FieldSchema *leftField, AggregateFunction func, int32_t paramId, 
		const std::string &op);
	virtual ~AggregateConstCondition();

	ConditionKind kind() override;

	AggregateFunction leftFunction() const;

private:
	AggregateFunction m_leftFunc;
};

class FieldCondition : public SimpleCondition
{
public:
	FieldCondition(FieldSchema *leftField, FieldSchema *rightField, const std::string &op);
	virtual ~FieldCondition();

	ConditionKind kind() override;
	bool match(SQLRecord *rec, MyVariants &params) override;
	void toString(std::string &result, SQLRecord *joinTableRec, 
		std::vector<int16_t>& paramIndex) override;

	FieldSchema *leftField() const;
	FieldSchema *rightField() const;

protected:
	virtual std::string sqlFieldName(bool left = true);

protected:
	FieldSchema *m_leftField;
	FieldSchema *m_rightField;
	BinaryCompareOperator *m_comparator;
};

class AggregateFieldCondition : public FieldCondition
{
public:
	AggregateFieldCondition(FieldSchema *leftField, FieldSchema *rightField, const std::string &op, 
		AggregateFunction leftFunc, AggregateFunction rightFunc = AggregateFunction::gfUnknown);
	virtual ~AggregateFieldCondition();

	ConditionKind kind() override;

	AggregateFunction leftFunction() const;
	AggregateFunction rightFunction() const;

protected:
	std::string sqlFieldName(bool left = true) override;

private:
	AggregateFunction m_leftFunc;
	AggregateFunction m_rightFunc;
};
