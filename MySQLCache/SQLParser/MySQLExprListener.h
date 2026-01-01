#pragma once

#include "antlr4-runtime.h"
#include "MySqlParserBaseListener.h"
#include "SQLGraph.h"
#include "SQLContext.h"
#include "Common.h"
#include <vector>
#include <functional>

enum class SQLParseState
{
	spsInit,
	spsSelectElement,
	spsFromClause,
	spsOrderBy,
	spsGroupBy,
	spsHaving
};

struct NormalTableSchemaInfo
{
	std::string name;
	SQLNormalTableSchema *schema = nullptr;
};

class MySQLExprListener : public MySqlParserBaseListener {
public:
	MySQLExprListener(SQLContext *context);
	virtual ~MySQLExprListener();

	virtual SQLNormalTableSchema *tableSchema(int index = 0) const = 0;

protected:
	SQLContext *m_context;
};

class MySQLSelectExprListener : public MySQLExprListener {
public:
	typedef std::function<void(SimpleCondition *)> ConditionIteration;

	MySQLSelectExprListener(SQLContext *context, const std::string &sql);
	virtual ~MySQLSelectExprListener();

	int selectFieldCount();
	FieldSchema *selectField(int index) const;

	int orderByFieldCount();
	const OrderFieldInfo &orderByField(int index) const;

	int groupByFieldCount();
	FieldSchema *groupByField(int index) const;

	int aggregateFieldCount();
	const AggregateFieldInfo &aggregateField(int index) const;

	template <typename IterationFunc>
	void forEachCondition(std::shared_ptr<Condition> conditionObj, const IterationFunc &iteration);
	void forEachCondition(std::shared_ptr<Condition> conditionObj, const ConditionIteration &iteration);

	std::shared_ptr<Condition> condition() const;
	std::shared_ptr<Condition> having() const;

	SQLJoinType joinType() const;
	int tableCount() const;
	SQLNormalTableSchema *tableSchema(int index = 0) const override;
	std::string tableName(int index) const;

	std::string getSqlColumnName(FieldSchema *fd);

	void enterAtomTableItem(MySqlParser::AtomTableItemContext * /*ctx*/) override;
	void enterFromClause(MySqlParser::FromClauseContext * /*ctx*/) override;
	void enterLogicalExpression(MySqlParser::LogicalExpressionContext * /*ctx*/) override;
	void enterPredicateExpression(MySqlParser::PredicateExpressionContext * /*ctx*/) override;
	void enterSelectElements(MySqlParser::SelectElementsContext * /*ctx*/) override;

	void enterInnerJoin(MySqlParser::InnerJoinContext * /*ctx*/) override;
	void enterStraightJoin(MySqlParser::StraightJoinContext * /*ctx*/) override;
	void enterOuterJoin(MySqlParser::OuterJoinContext * /*ctx*/) override;
	void enterNaturalJoin(MySqlParser::NaturalJoinContext * /*ctx*/) override;

	void enterOrderByClause(MySqlParser::OrderByClauseContext * /*ctx*/) override;
	void enterOrderByExpression(MySqlParser::OrderByExpressionContext * /*ctx*/) override;

	void enterGroupByClause(MySqlParser::GroupByClauseContext * /*ctx*/) override;
	void enterGroupByItem(MySqlParser::GroupByItemContext * /*ctx*/) override;

	void enterHavingClause(MySqlParser::HavingClauseContext * /*ctx*/) override;

	void enterSqlStatement(MySqlParser::SqlStatementContext * /*ctx*/) override;
	void exitSqlStatement(MySqlParser::SqlStatementContext * /*ctx*/) override;

	void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override;

	void enterUnionStatement(MySqlParser::UnionStatementContext * /*ctx*/) override;
	void enterSubqueryTableItem(MySqlParser::SubqueryTableItemContext * /*ctx*/) override;
private:
	bool isColumnName(antlr4::tree::ParseTree *ctx);
	bool isConstant(antlr4::tree::ParseTree *ctx);
	bool isAggregateColumn(antlr4::tree::ParseTree *ctx);

	Condition *parseLogicalExpressionContext(MySqlParser::LogicalExpressionContext *ctx, int startChild = 0);
	Condition *parsePredicateExpressionContext(MySqlParser::PredicateExpressionContext *ctx);

	Condition *parseBinaryCompareContext(MySqlParser::BinaryComparisonPredicateContext *ctx);
	Condition *buildBinaryCompareCondition(MySqlParser::FullColumnNameExpressionAtomContext *left,
		antlr4::tree::ParseTree *right, const std::string &op);
	Condition *buildBinaryCompareCondition(MySqlParser::AggregateFunctionCallContext *left, 
		antlr4::tree::ParseTree *right, const std::string &op);
	Condition *buildMultiConstCompareCondition(MySqlParser::FullColumnNameExpressionAtomContext *left, 
		int32_t paramSize, const std::string &op);

	Condition *parseBetweenCompareContext(MySqlParser::BetweenPredicateContext *ctx);
	Condition *parseInCompareContext(MySqlParser::InPredicateContext *ctx);

	void parseSelectColumnContext(MySqlParser::SelectColumnElementContext *ctx);
	void parseSelectColumns();
	void parseAggregateColumns();
	void parseSelectFunctionContext(MySqlParser::SelectFunctionElementContext *ctx);

	FieldSchema *findField(const std::string &name);

	void dfs_condition(Condition *cond, const ConditionIteration &iteration);

private:
	std::vector<FieldSchema *> m_selectFields;
	std::vector<OrderFieldInfo> m_orderByFields;
	std::vector<FieldSchema *> m_groupByFields;
	std::vector<AggregateFieldInfo> m_aggregateFieldInfo;
	std::vector<NormalTableSchemaInfo> m_tableSchemas;
	std::vector<std::string> m_selectColumns;
	std::vector<std::tuple<std::string, std::string, std::string> > m_aggregateInfoStrs;

	std::shared_ptr<Condition> m_where;
	std::shared_ptr<Condition> m_having;
	int32_t m_paramId;
	SQLParseState m_parseState;
	SQLJoinType m_joinType;
	std::unordered_map<std::string, std::string> m_sqlColStrMaps;
	std::unordered_map<FieldSchema *, std::string> m_sqlColMaps;
};

class MySQLUpdateExprListener : public MySQLExprListener {
public:
	MySQLUpdateExprListener(SQLContext *context);
	virtual ~MySQLUpdateExprListener();

	void enterTableName(MySqlParser::TableNameContext * /*ctx*/) override;
	void enterUpdatedElement(MySqlParser::UpdatedElementContext * /*ctx*/) override;
	void enterSingleUpdateStatement(MySqlParser::SingleUpdateStatementContext * /*ctx*/) override;

	int updateFieldCount() const;
	FieldSchema *updateField(int index) const;

	SQLNormalTableSchema *tableSchema(int index = 0) const override;

private:
	SQLNormalTableSchema *m_tableSchema;
	std::vector<FieldSchema *> m_updateFields;
};

class MySQLInsertExprListener : public MySQLExprListener {
public:
	MySQLInsertExprListener(SQLContext *context);
	virtual ~MySQLInsertExprListener();

	void enterTableName(MySqlParser::TableNameContext * /*ctx*/) override;
	void enterFullColumnNameList(MySqlParser::FullColumnNameListContext * /*ctx*/) override;
	void enterExpressionOrDefault(MySqlParser::ExpressionOrDefaultContext * /*ctx*/) override;

	int recordCount() const;
	int insertFieldCount() const;
	FieldSchema *insertField(int index) const;
	const MyVariant &insertValue(int index) const;

	SQLNormalTableSchema *tableSchema(int index = 0) const override;
	bool valid() const;

private:
	SQLNormalTableSchema *m_tableSchema;
	std::vector<FieldSchema *> m_fields;
	std::vector<MyVariant> m_values;
	int m_curFieldIndex;
	bool m_valid;
};

class MySQLDeleteExprListener : public MySQLExprListener {
public:
	MySQLDeleteExprListener(SQLContext *context);
	virtual ~MySQLDeleteExprListener();

	void enterTableName(MySqlParser::TableNameContext * /*ctx*/) override;
	void enterSingleDeleteStatement(MySqlParser::SingleDeleteStatementContext * /*ctx*/) override;

	SQLNormalTableSchema *tableSchema(int index = 0) const override;

private:
	SQLNormalTableSchema *m_tableSchema;
};

template<typename IterationFunc>
inline void MySQLSelectExprListener::forEachCondition(std::shared_ptr<Condition> conditionObj, 
	const IterationFunc &iteration)
{
	ConditionIteration func = iteration;
	forEachCondition(conditionObj, func);
}
