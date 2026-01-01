#include "SQLGraph.h"
#include "SQLTable.h"
#include "MathUtils.h"
#include "StrUtils.h"
#include "Consts.h"
#include "Common.h"
#include "SQLTableContainer.h"

using namespace std;

const int BIT_Query = 0;
const int BIT_WHERE = 1;
const int BIT_ORDER = 2;

const string OP_AND = "AND";
const string OP_BETWEEN = "BETWEEN";
const string OP_EQUAL = "=";
const string OP_GREATER = ">";
const string OP_GREATER_EQUAL = ">=";
const string OP_LESS = "<";
const string OP_LESS_EQUAL = "<=";
const string OP_IN = "IN";

SQLGraph::SQLGraph(int threadIndex) :
	m_threadIndex(threadIndex)
{
}

SQLGraph::~SQLGraph()
{
	FOR_EACH(i, m_vtxs) {
		delete i->second;
	}
}

SQLVertex *SQLGraph::addVertex(FieldSchema *fieldSchema)
{
	intptr_t schema = reinterpret_cast<intptr_t>(fieldSchema);
	SQLVertex *result = findVertex(schema);
	if (!result) {
		result = new SQLFieldVertex(fieldSchema);
		m_vtxs[schema] = result;
	}

	return result;
}

SQLVertex *SQLGraph::addVertex(SQLTableSchema *tableSchema)
{
	intptr_t schema = reinterpret_cast<intptr_t>(tableSchema);
	SQLVertex *result = findVertex(schema);
	if (!result) {
		result = new SQLSchemaVertex(tableSchema);
		m_vtxs[schema] = result;
	}

	return result;
}

void SQLGraph::freeVertex(SQLVertex *vtx)
{
	intptr_t schema = 0;
	if (vtx->type() == VertexType::vtSchema) {
		schema = reinterpret_cast<intptr_t>(static_cast<SQLSchemaVertex *>(vtx)->schema());
	}
	else if (vtx->type() == VertexType::vtField) {
		schema = reinterpret_cast<intptr_t>(static_cast<SQLFieldVertex *>(vtx)->fieldSchema());
	}
	m_vtxs.erase(schema);
	delete vtx;
}

SQLVertex *SQLGraph::findVertex(intptr_t schema)
{
	auto n = m_vtxs.find(schema);
	if (n != m_vtxs.end()) {
		return n->second;
	}

	return nullptr;
}

SimpleCondition::SimpleCondition(const std::string &op) :
	Condition(op)
{
}

SimpleCondition::~SimpleCondition()
{
}

ConstCondition::ConstCondition(FieldSchema *leftField, int32_t startParamId, int32_t endParamId,
	const std::string &op) :
	SimpleCondition(op),
	m_leftField(leftField),
	m_startParamId(startParamId),
	m_endParamId(endParamId)
{
	m_comparator = BinaryCompareOperator::buildOperator(op);
}

ConstCondition::~ConstCondition()
{
}

int32_t ConstCondition::startParamID() const
{
	return m_startParamId;
}

int32_t ConstCondition::endParamID() const
{
	return m_endParamId;
}

ConditionKind ConstCondition::kind()
{
	return ConditionKind::ckConst;
}

bool ConstCondition::match(SQLRecord *rec, MyVariants &params)
{
	if (static_cast<SQLNormalTable *>(rec->table())->name() != m_leftField->tableName()) {
		return true;
	}

	if (m_startParamId == m_endParamId) {
		return m_comparator->compare(rec->value(m_leftField->name()), 
			params.variant(m_startParamId));
	}
	else {
		MyVariants v;
		for (int i = m_startParamId; i <= m_endParamId; ++i) {
			v.add(params.variant(i));
		}

		return m_comparator->compare(rec->value(m_leftField->name()), v);
	}
}

void ConstCondition::toString(std::string &result, SQLRecord *joinTableRec,
	std::vector<int16_t>& paramIndex)
{
	if (static_cast<SQLNormalTable *>(joinTableRec->table())->name() == m_leftField->tableName()) {
		result.append("TRUE");
		return;
	}

	for (int16_t i = m_startParamId; i <= m_endParamId; ++i) {
		paramIndex.push_back(i);
	}

	StrUtils::append(result, m_leftField->name(), " ", op(), " ");
	if (op() == OP_EQUAL) {
		result.append("?");
	}
	else if (op() == OP_BETWEEN) {
		StrUtils::append(result, "? AND ?");
	}
	else if (op() == OP_IN) {
		result.append("(");
		for (int i = m_startParamId; i <= m_endParamId; ++i) {
			result.append("?");
			if (i != m_endParamId) {
				result.append(",");
			}
		}
		result.append(")");
	}
}

FieldSchema *ConstCondition::leftField() const
{
	return m_leftField;
}

FieldCondition::FieldCondition(FieldSchema *leftField, FieldSchema *rightField,
	const std::string &op) :
	SimpleCondition(op),
	m_leftField(leftField),
	m_rightField(rightField)
{
	m_comparator = BinaryCompareOperator::buildOperator(op);
}

FieldCondition::~FieldCondition()
{
}

ConditionKind FieldCondition::kind()
{
	return ConditionKind::ckField;
}

bool FieldCondition::match(SQLRecord *rec, MyVariants &params)
{
	if (m_leftField->tableName() == m_rightField->tableName()) {
		return m_comparator->compare(rec->value(m_leftField->name()), rec->value(m_rightField->name()));
	}
	else {
		return true;
	}
}

void FieldCondition::toString(std::string &result, SQLRecord *joinTableRec,
	std::vector<int16_t>& paramIndex)
{
	std::string joinTableName = static_cast<SQLNormalTable *>(joinTableRec->table())->normalSchema()->name();
	if (m_leftField->tableName() == joinTableName) {
		result.append(joinTableRec->strValue(m_leftField->name()));
	}
	else {
		result.append(m_leftField->name());
	}

	result.append(" ").append(op());

	if (m_rightField->tableName() == joinTableName) {
		result.append(joinTableRec->strValue(m_rightField->name()));
	}
	else {
		result.append(m_rightField->name());
	}
}

FieldSchema *FieldCondition::leftField() const
{
	return m_leftField;
}

FieldSchema *FieldCondition::rightField() const
{
	return m_rightField;
}

std::string FieldCondition::sqlFieldName(bool left)
{
	if (left) {
		return m_leftField->name();
	}
	else {
		return m_rightField->name();
	}
}

Condition::Condition(const std::string &op) :
	m_op(op)
{
}

Condition::~Condition()
{
}

const std::string &Condition::op() const
{
	return m_op;
}

void Condition::setOp(const std::string &op)
{
	m_op = StrUtils::toUpper(op);
}

BinaryCondition::BinaryCondition(const std::string &op) :
	Condition(op),
	m_left(nullptr),
	m_right(nullptr)
{
}

BinaryCondition::~BinaryCondition()
{
	if (m_left) {
		delete m_left;
		m_left = nullptr;
	}

	if (m_right) {
		delete m_right;
		m_right = nullptr;
	}
}

ConditionKind BinaryCondition::kind()
{
	return ConditionKind::ckBinary;
}

Condition *BinaryCondition::left()
{
	return m_left;
}

void BinaryCondition::setLeft(Condition *condition)
{
	m_left = condition;
}

Condition *BinaryCondition::right()
{
	return m_right;
}

void BinaryCondition::setRight(Condition *condition)
{
	m_right = condition;
}

bool BinaryCondition::match(SQLRecord *rec, MyVariants &params)
{
	if (op() == OP_AND) {
		return m_left->match(rec, params) && m_right->match(rec, params);
	}
	else if (op() == "OR") {
		return m_left->match(rec, params) || m_right->match(rec, params);
	}

	return false;
}

void BinaryCondition::toString(std::string &result, SQLRecord *joinTableRec,
	std::vector<int16_t>& paramIndex)
{
	result.append("(");
	m_left->toString(result, joinTableRec, paramIndex);
	result.append(" ").append(op()).append(" ");
	m_right->toString(result, joinTableRec, paramIndex);
	result.append(")");
}

SQLVertex::SQLVertex()
{
}

SQLVertex::~SQLVertex()
{
}

SQLSchemaVertex::SQLSchemaVertex(SQLTableSchema *schema) :
	SQLVertex(),
	m_schema(schema),
	m_indexCondition(nullptr),
	m_index(nullptr)
{
}

SQLSchemaVertex::~SQLSchemaVertex()
{
	if (m_index) {
		delete m_index;
	}
}

VertexType SQLSchemaVertex::type()
{
	return VertexType::vtSchema;
}

SQLTableSchema *SQLSchemaVertex::schema() const
{
	return m_schema;
}

void SQLSchemaVertex::setSchema(SQLTableSchema *schema)
{
	m_schema = schema;
}

std::shared_ptr<Condition> SQLSchemaVertex::condition() const
{
	return m_where;
}

void SQLSchemaVertex::setCondition(std::shared_ptr<Condition> c)
{
	m_where = c;
	if (m_where) {
		buildIndexCondition();
	}
}

void SQLSchemaVertex::addTable(SQLTable *table, uint32_t tableId, int thIndex)
{
	if (m_indexCondition) {
		addTableToIndex(table, tableId, thIndex);
	}
	
	m_tables[table->params()] = tableId;
}

void SQLSchemaVertex::clearTable(int thIndex)
{
	SQLTableContainer *container = SQLTableContainer::instance(thIndex);
	FOR_EACH(i, m_tables) {
		container->removeTable(i->second);
	}
	m_tables.clear();
	if (m_indexCondition) {
		m_index->clear();
	}
}

int SQLSchemaVertex::tableCount() const
{
	return m_tables.size();
}

SQLTable *SQLSchemaVertex::findTable(const MyVariants &key, int thIndex)
{
	auto i = m_tables.find(key);
	if (i != m_tables.end()) {
		SQLTable *tbl = SQLTableContainer::instance(thIndex)->getTable(i->second);
		if (!tbl) {
			// is free for out of memory
			m_tables.erase(i);
		}
		return tbl;
	}

	return nullptr;
}

std::vector<SQLTable *> SQLSchemaVertex::findTable(SQLRecord *rec, int thIndex)
{
	vector<SQLTable *> result;
	if (!m_indexCondition) {
		FOR_EACH(i, m_tables) {
			SQLTable *tbl = SQLTableContainer::instance(thIndex)->getTable(i->second);
			if (tbl) {
				result.push_back(tbl);
			}
		}
	}

	if (!m_where) {
		return result;
	}

	if (m_indexCondition) {
		vector<uint32_t> srcTables = findTableByIndex(rec);
		for (int i = 0; i < srcTables.size(); ++i) {
			SQLTable* srcTable = SQLTableContainer::instance(thIndex)->getTable(srcTables[i]);
			if (srcTable && m_where->match(rec, srcTable->params())) {
				result.push_back(srcTable);
			}
		}
	}
	else {
		for (int i = result.size() - 1; i >= 0; --i) {
			SQLTable* curTable = result[i];
			if (!m_where->match(rec, curTable->params())) {
				result.erase(result.begin() + i);
			}
		}
	}
	
	return result;
}

void SQLSchemaVertex::buildIndexCondition()
{
	innerBuildIndexCondition(m_where.get());
	if (m_indexCondition) {
		if (m_indexCondition->op() == OP_EQUAL) {
			m_index = SQLTableIndexFactory::createIndex(TableIndexKind::tikEqual);
		}
		else if (m_indexCondition->op() == OP_GREATER) {
			m_index = SQLTableIndexFactory::createIndex(TableIndexKind::tikGreater);
		}
		else if (m_indexCondition->op() == OP_GREATER_EQUAL) {
			m_index = SQLTableIndexFactory::createIndex(TableIndexKind::tikGreaterEqual);
		}
		else if (m_indexCondition->op() == OP_LESS) {
			m_index = SQLTableIndexFactory::createIndex(TableIndexKind::tikLess);
		}
		else if (m_indexCondition->op() == OP_LESS_EQUAL) {
			m_index = SQLTableIndexFactory::createIndex(TableIndexKind::tikLessEqual);
		}
		else if (m_indexCondition->op() == OP_BETWEEN) {
			m_index = SQLTableIndexFactory::createIndex(TableIndexKind::tikBetween);
		}
	}
}

void SQLSchemaVertex::innerBuildIndexCondition(Condition *cond)
{
	if (cond->kind() == ConditionKind::ckBinary) {
		BinaryCondition *bCond = static_cast<BinaryCondition *>(cond);
		if (bCond->op() != OP_AND) {
			return;
		}

		innerBuildIndexCondition(bCond->left());
		innerBuildIndexCondition(bCond->right());
	}
	else if (cond->kind() == ConditionKind::ckConst) {
		ConstCondition *condition = static_cast<ConstCondition *>(cond);
		DataType dt = condition->leftField()->dataType();
		if (dt == DataType::dtInt || dt == DataType::dtSmallInt || dt == DataType::dtBigInt
			|| dt == DataType::dtString) {
			if (cond->op() == OP_EQUAL) {
				if (!m_indexCondition || m_indexCondition->op() != OP_EQUAL) {
					m_indexCondition = condition;
				}
			}
			else if (cond->op() == OP_LESS || cond->op() == OP_LESS_EQUAL 
				|| cond->op() == OP_GREATER || cond->op() == OP_GREATER_EQUAL) {
				if (!m_indexCondition || m_indexCondition->op() == OP_BETWEEN) {
					m_indexCondition = condition;
				}
			}
			else if (cond->op() == OP_BETWEEN) {
				if (!m_indexCondition) {
					m_indexCondition = condition;
				}
			}
		}
	}
}

std::vector<uint32_t> SQLSchemaVertex::findTableByIndex(SQLRecord *rec)
{
	return m_index->find(rec->value(m_indexCondition->leftField()->name()));
}

void SQLSchemaVertex::addTableToIndex(SQLTable *table, uint32_t tableId, int thIndex)
{
	int s = m_indexCondition->startParamID();
	int e = m_indexCondition->endParamID();
	if (m_indexCondition->op() == OP_BETWEEN) {
		m_index->add(table->params().variant(s), table->params().variant(e), tableId);
	}
	else {
		for (int i = s; i <= e; ++i) {
			m_index->add(table->params().variant(i), tableId);
		}
	}
}

SQLEdge::SQLEdge() :
	m_relations(0)
{
}

SQLEdge::~SQLEdge()
{
}

void SQLEdge::setQuery(bool v)
{
	RelationUtils::setQuery(m_relations, v);
}

bool SQLEdge::isQuery() const
{
	return RelationUtils::isQuery(m_relations);
}

void SQLEdge::setWhere(bool v)
{
	RelationUtils::setWhere(m_relations, v);
}

bool SQLEdge::isWhere() const
{
	return RelationUtils::isWhere(m_relations);
}

void SQLEdge::setOrder(bool v)
{
	RelationUtils::setOrder(m_relations, v);
}

bool SQLEdge::isOrder() const
{
	return RelationUtils::isOrder(m_relations);
}

uint8_t SQLEdge::relations() const
{
	return m_relations;
}

SQLVertex *SQLEdge::to() const
{
	return m_to;
}

void SQLEdge::setTo(SQLVertex *to)
{
	m_to = to;
}

SQLFieldVertex::SQLFieldVertex(FieldSchema *schema) :
	SQLVertex(),
	m_fieldSchema(schema)
{
}

SQLFieldVertex::~SQLFieldVertex()
{
	for (int i = 0; i < m_edges.size(); ++i) {
		delete m_edges[i];
	}
}

VertexType SQLFieldVertex::type()
{
	return VertexType::vtField;
}

FieldSchema *SQLFieldVertex::fieldSchema() const
{
	return m_fieldSchema;
}

void SQLFieldVertex::setFieldSchema(FieldSchema *schema)
{
	m_fieldSchema = schema;
}

SQLEdge *SQLFieldVertex::appendEdge(SQLVertex *vtx)
{
	SQLEdge *edge = new SQLEdge();
	edge->setTo(vtx);
	m_edges.push_back(edge);
	return edge;
}

int SQLFieldVertex::edgeCount() const
{
	return m_edges.size();
}

SQLEdge *SQLFieldVertex::edge(int index)
{
	return m_edges.at(index);
}

BinaryCompareOperator *BinaryCompareOperator::buildOperator(const std::string &op)
{
	if (op == "=") {
		return new EqualOperator();
	}
	else if (op == "<>") {
		return new UnEqualOperator();
	}
	else if (op[0] == '<') {
		LesserOperator *oper = new LesserOperator();
		oper->setIncludeEqual(op.length() >= 2 && op[1] == '=');
		return oper;
	}
	else if (op[0] == '>') {
		GreaterOperator *oper = new GreaterOperator();
		oper->setIncludeEqual(op.length() >= 2 && op[1] == '=');
		return oper;
	}
	else if (op == OP_BETWEEN) {
		return new BetweenOperator();
	}
	else if (op == OP_IN) {
		return new InOperator();
	}

	return nullptr;
}

GreaterOperator::GreaterOperator() :
	BinaryCompareOperator(),
	m_includeEqual(false)
{
}

GreaterOperator::~GreaterOperator()
{
}

bool GreaterOperator::compare(const MyVariant &v1, const MyVariant &v2)
{
	return m_includeEqual ? v1 >= v2 : v1 > v2;
}

bool GreaterOperator::includeEqual() const
{
	return m_includeEqual;
}

void GreaterOperator::setIncludeEqual(bool e)
{
	m_includeEqual = e;
}

LesserOperator::LesserOperator() :
	BinaryCompareOperator(),
	m_includeEqual(false)
{
}

LesserOperator::~LesserOperator()
{
}

bool LesserOperator::compare(const MyVariant &v1, const MyVariant &v2)
{
	return m_includeEqual ? v1 <= v2 : v1 < v2;
}

bool LesserOperator::includeEqual() const
{
	return m_includeEqual;
}

void LesserOperator::setIncludeEqual(bool e)
{
	m_includeEqual = e;
}

bool EqualOperator::compare(const MyVariant &v1, const MyVariant &v2)
{
	return v1 == v2;
}

void RelationUtils::setQuery(uint8_t &relation, bool v)
{
	set(relation, BIT_Query, v);
}

bool RelationUtils::isQuery(uint8_t relation)
{
	return get(relation, BIT_Query);
}

void RelationUtils::setWhere(uint8_t &relation, bool v)
{
	set(relation, BIT_WHERE, v);
}

bool RelationUtils::isWhere(uint8_t relation)
{
	return get(relation, BIT_WHERE);
}

void RelationUtils::setOrder(uint8_t &relation, bool v)
{
	set(relation, BIT_ORDER, v);
}

bool RelationUtils::isOrder(uint8_t relation)
{
	return get(relation, BIT_ORDER);
}

void RelationUtils::set(uint8_t &dstRelation, uint8_t srcRelation)
{
	dstRelation |= srcRelation;
}

void RelationUtils::set(uint8_t &relation, int bitIndex, bool v)
{
	if (v) {
		relation |= (1 << bitIndex);
	}
	else {
		relation &= ~(1 << bitIndex);
	}
}

bool RelationUtils::get(uint8_t relation, int bitIndex)
{
	return (relation & (1 << bitIndex)) != 0;
}

AggregateConstCondition::AggregateConstCondition(FieldSchema *leftField, AggregateFunction func,
	int32_t paramId, const std::string &op) :
	ConstCondition(leftField, paramId, paramId, op),
	m_leftFunc(func)
{
}

AggregateConstCondition::~AggregateConstCondition()
{
}

ConditionKind AggregateConstCondition::kind()
{
	return ConditionKind::ckAggregateConst;
}

AggregateFunction AggregateConstCondition::leftFunction() const
{
	return m_leftFunc;
}

AggregateFieldCondition::AggregateFieldCondition(FieldSchema *leftField, FieldSchema *rightField, 
	const std::string &op, AggregateFunction leftFunc, AggregateFunction rightFunc) :
	FieldCondition(leftField, rightField, op),
	m_leftFunc(leftFunc),
	m_rightFunc(rightFunc)
{
}

AggregateFieldCondition::~AggregateFieldCondition()
{
}

ConditionKind AggregateFieldCondition::kind()
{
	return ConditionKind::ckAggregateField;
}

AggregateFunction AggregateFieldCondition::leftFunction() const
{
	return m_leftFunc;
}

AggregateFunction AggregateFieldCondition::rightFunction() const
{
	return m_rightFunc;
}

std::string AggregateFieldCondition::sqlFieldName(bool left)
{
	if (left) {
		return StrUtils::join(HAVING_AGGREGATE_SUFFIX, functionStr(m_leftFunc),
					SUFFIX_SEPARATOR, m_leftField->name());
	}
	else {
		return StrUtils::join(HAVING_AGGREGATE_SUFFIX, functionStr(m_rightFunc),
					SUFFIX_SEPARATOR, m_rightField->name());
	}
}

bool UnEqualOperator::compare(const MyVariant &v1, const MyVariant &v2)
{
	return !(v1 == v2);
}

bool BetweenOperator::compare(const MyVariant& v1, const MyVariant& v2)
{
	return false;
}

bool BetweenOperator::compare(const MyVariant &v1, const MyVariants &v2)
{
	return v1 >= v2.variant(0) && v1 <= v2.variant(1);
}

bool InOperator::compare(const MyVariant& v1, const MyVariant& v2)
{
	return false;
}

bool InOperator::compare(const MyVariant &v1, const MyVariants &v2)
{
	for (int i = 0; i < v2.count(); ++i) {
		if (v2.variant(i) == v1) {
			return true;
		}
	}

	return false;
}
