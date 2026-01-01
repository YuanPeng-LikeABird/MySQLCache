#include "MySQLExprListener.h"
#include "SQLTableSchema.h"
#include "StrUtils.h"
#include "SQLParseException.h"
#include <exception>
#include <iostream>
#include <stdlib.h>

using namespace antlr4;

const std::string ALL_COLUMN_WORD = "*";

const char *INVALID_SQL_SYNTAX = "Can't cache SQL syntax";
const char *UNSUPPORTED_JOIN_TYPE = "Not Support Join Type";

MyVariant getFieldValue(const std::string &valueStr, DataType dataType) 
{
	if (valueStr == "'?'") {
		return "?";
	}

	switch (dataType) {
		case DataType::dtBoolean:
		{
			std::string value = StrUtils::toLower(valueStr);
			if (value == "true") {
				return true;
			}
			else if (value == "false") {
				return false;
			}
			else {
				throw SQLParseException("invalid bool");
			}
		}
		case DataType::dtSmallInt:
		case DataType::dtInt:
		case DataType::dtBigInt:
		case DataType::dtFloat:
		case DataType::dtDouble:
		{
			char *endPtr;
			double value = strtod(valueStr.c_str(), &endPtr);
			if (*endPtr == '\0') {
				throw SQLParseException("invalid double");
			}
			return value;
		}
		case DataType::dtString:
		{
			if (valueStr.size() < 2 || (valueStr[0] != '\'' && valueStr[0] != '"') ||
				(valueStr[valueStr.size() - 1] != '\'' && valueStr[valueStr.size() - 1] != '"')) {
				throw SQLParseException("invalid string");
			}
			return valueStr.substr(1, valueStr.length() - 2);
		}
		default:
			throw SQLParseException("invalid const");
			break;
	}

	return MyVariant();
}

MySQLSelectExprListener::MySQLSelectExprListener(SQLContext *context, const std::string &sql) :
	MySQLExprListener(context)
{
}

MySQLSelectExprListener::~MySQLSelectExprListener()
{
}

void MySQLSelectExprListener::enterAtomTableItem(MySqlParser::AtomTableItemContext *ctx)
{
	if (m_parseState == SQLParseState::spsFromClause) {
		std::string tableName = ctx->children[0]->getText();
		SQLNormalTableSchema *tableSchema = m_context->findTable(tableName);
		if (tableSchema) {
			if (ctx->alias) {
				tableName = ctx->alias->getText();
			}

			NormalTableSchemaInfo info;
			info.schema = tableSchema;
			info.name = tableName;
			m_tableSchemas.push_back(info);
		}
	}
}

void MySQLSelectExprListener::enterInnerJoin(MySqlParser::InnerJoinContext *)
{
	m_joinType = SQLJoinType::sjtInner;
}

void MySQLSelectExprListener::enterStraightJoin(MySqlParser::StraightJoinContext *)
{
	throw SQLParseException(UNSUPPORTED_JOIN_TYPE);
}

void MySQLSelectExprListener::enterOuterJoin(MySqlParser::OuterJoinContext *)
{
	throw SQLParseException(UNSUPPORTED_JOIN_TYPE);
}

void MySQLSelectExprListener::enterNaturalJoin(MySqlParser::NaturalJoinContext *)
{
	throw SQLParseException(UNSUPPORTED_JOIN_TYPE);
}

void MySQLSelectExprListener::enterFromClause(MySqlParser::FromClauseContext *ctx)
{
	m_parseState = SQLParseState::spsFromClause;
}

void MySQLSelectExprListener::enterLogicalExpression(MySqlParser::LogicalExpressionContext *ctx)
{
	if (m_parseState == SQLParseState::spsFromClause && !m_where) {
		std::string ttt = ctx->getText();
		m_where = std::shared_ptr<Condition>(parseLogicalExpressionContext(ctx));
		if (!m_where) {
			throw SQLParseException("WHERE clause is not cacheable");
		}
	}
	else if (m_parseState == SQLParseState::spsHaving && !m_having) {
		m_having = std::shared_ptr<Condition>(parseLogicalExpressionContext(ctx));
		if (!m_having) {
			throw SQLParseException("HAVING clause is not cacheable");
		}
	}
}

void MySQLSelectExprListener::enterPredicateExpression(MySqlParser::PredicateExpressionContext *ctx)
{
	if (m_parseState == SQLParseState::spsFromClause && !m_where) {
		m_where = std::shared_ptr<Condition>(parsePredicateExpressionContext(ctx));
		if (!m_where) {
			throw SQLParseException("WHERE clause is not cacheable");
		}
	}
	else if (m_parseState == SQLParseState::spsHaving && !m_having) {
		m_having = std::shared_ptr<Condition>(parsePredicateExpressionContext(ctx));
		if (!m_having) {
			throw SQLParseException("HAVING clause is not cacheable");
		}
	}
}

void MySQLSelectExprListener::enterSelectElements(MySqlParser::SelectElementsContext *ctx)
{
	m_selectColumns.clear();
	
	if (ctx->STAR()) {
		m_selectColumns.push_back(ALL_COLUMN_WORD);
	}
	else {
		for (int i = 0; i < ctx->children.size(); ++i) {
			auto ch = ctx->children[i];
			if (auto *columnCtx = dynamic_cast<MySqlParser::SelectColumnElementContext *>(ch); columnCtx != nullptr) {
				parseSelectColumnContext(columnCtx);
			}
			else if (auto *columnCtx = dynamic_cast<MySqlParser::SelectStarElementContext *>(ch); columnCtx != nullptr) {
				m_selectColumns.push_back(ALL_COLUMN_WORD);
			}
			else if (auto *funcCtx = dynamic_cast<MySqlParser::SelectFunctionElementContext *>(ch); funcCtx != nullptr) {
				parseSelectFunctionContext(funcCtx);
			}
		}
	}

	m_parseState = SQLParseState::spsSelectElement;
}

void MySQLSelectExprListener::enterOrderByClause(MySqlParser::OrderByClauseContext *ctx)
{
	m_parseState = SQLParseState::spsOrderBy;
}

void MySQLSelectExprListener::enterOrderByExpression(MySqlParser::OrderByExpressionContext *ctx)
{
	std::string columnName = ctx->expression()->getText();
	OrderFieldInfo fieldInfo;
	fieldInfo.field = findField(columnName);
	if (!fieldInfo.field) {
		throw SQLParseException(INVALID_SQL_SYNTAX);
	}

	if (ctx->DESC()) {
		fieldInfo.order = OrderType::otDesc;
	}
	else {
		fieldInfo.order = OrderType::otAsc;
	}

	m_orderByFields.push_back(fieldInfo);
}

void MySQLSelectExprListener::enterGroupByClause(MySqlParser::GroupByClauseContext *ctx)
{
	m_parseState = SQLParseState::spsGroupBy;
}

void MySQLSelectExprListener::enterGroupByItem(MySqlParser::GroupByItemContext *ctx)
{
	std::string columnName = ctx->getText();
	FieldSchema *field = findField(columnName);
	if (!field) {
		throw SQLParseException(INVALID_SQL_SYNTAX);
	}

	m_groupByFields.push_back(field);
}

void MySQLSelectExprListener::enterHavingClause(MySqlParser::HavingClauseContext *ctx)
{
	m_parseState = SQLParseState::spsHaving;
}

void MySQLSelectExprListener::enterSqlStatement(MySqlParser::SqlStatementContext *)
{
	m_paramId = 0;
	m_where = nullptr;
	m_having = nullptr;
	m_parseState = SQLParseState::spsInit;
	m_joinType = SQLJoinType::sjtNull;
}

void MySQLSelectExprListener::exitSqlStatement(MySqlParser::SqlStatementContext *)
{
	parseSelectColumns();
	parseAggregateColumns();
	if (m_tableSchemas.size() > 1) {
		m_joinType = SQLJoinType::sjtInner;
	}
}

void MySQLSelectExprListener::visitErrorNode(antlr4::tree::ErrorNode *e)
{
	throw SQLParseException(e->getText().c_str());
}

void MySQLSelectExprListener::enterUnionStatement(MySqlParser::UnionStatementContext *)
{
	throw SQLParseException("Not Support Union");
}

void MySQLSelectExprListener::enterSubqueryTableItem(MySqlParser::SubqueryTableItemContext *)
{
	throw SQLParseException("Not Support Subquery");
}

bool MySQLSelectExprListener::isColumnName(antlr4::tree::ParseTree *ctx)
{
	while (ctx->children.size() == 1) {
		ctx = ctx->children[0];
		if (dynamic_cast<MySqlParser::FullColumnNameExpressionAtomContext *>(ctx)) {
			return true;
		}
	}

	return false;
}

bool MySQLSelectExprListener::isConstant(antlr4::tree::ParseTree *ctx)
{
	while (ctx->children.size() == 1) {
		ctx = ctx->children[0];
		if (dynamic_cast<MySqlParser::ConstantExpressionAtomContext *>(ctx)) {
			return true;
		}
	}

	return false;
}

bool MySQLSelectExprListener::isAggregateColumn(antlr4::tree::ParseTree *ctx)
{
	auto predicateCtx = dynamic_cast<MySqlParser::ExpressionAtomPredicateContext *>(ctx);
	if (predicateCtx) {
		auto callExprCtx = dynamic_cast<MySqlParser::FunctionCallExpressionAtomContext *>(predicateCtx->children[0]);
		return callExprCtx && dynamic_cast<MySqlParser::AggregateFunctionCallContext *>(callExprCtx->children[0]);
	}

	return false;
}

Condition *MySQLSelectExprListener::parseLogicalExpressionContext(MySqlParser::LogicalExpressionContext *ctx, int startChild)
{
	if (ctx->children.size() > startChild + 1) {
		auto *operCtx = ctx->children[1];
		std::string oper = dynamic_cast<MySqlParser::LogicalOperatorContext *>(operCtx)->getText();
		BinaryCondition *result = new BinaryCondition(oper);

		auto *leftCtx = ctx->children[0];
		Condition *leftCondition = nullptr;
		if (auto *loExprCtx = dynamic_cast<MySqlParser::LogicalExpressionContext *>(leftCtx); loExprCtx != nullptr) {
			leftCondition = parseLogicalExpressionContext(loExprCtx);
		}
		else if (auto *preExprCtx = dynamic_cast<MySqlParser::PredicateExpressionContext *>(leftCtx); preExprCtx != nullptr) {
			leftCondition = parsePredicateExpressionContext(preExprCtx);
		}
		else {
			throw SQLParseException(StrUtils::join("unsupport operation : ", oper));
		}
		result->setLeft(leftCondition);

		Condition *rightCondition = parseLogicalExpressionContext(ctx, startChild + 2);
		result->setRight(rightCondition);
		return result;
	}
	else {
		auto *exprCtx = ctx->children[startChild];
		if (auto *loExprCtx = dynamic_cast<MySqlParser::LogicalExpressionContext *>(exprCtx); loExprCtx != nullptr) {
			return parseLogicalExpressionContext(loExprCtx);
		}
		else if (auto *preExprCtx = dynamic_cast<MySqlParser::PredicateExpressionContext *>(exprCtx); preExprCtx != nullptr) {
			return parsePredicateExpressionContext(preExprCtx);
		}

		return nullptr;
	}
}

Condition *MySQLSelectExprListener::parsePredicateExpressionContext(MySqlParser::PredicateExpressionContext *ctx)
{
	if (auto *binaryCompareCtx = dynamic_cast<MySqlParser::BinaryComparisonPredicateContext *>(ctx->children[0]); binaryCompareCtx != nullptr) {
		return parseBinaryCompareContext(binaryCompareCtx);
	}
	else if (auto *betweenCompareCtx = dynamic_cast<MySqlParser::BetweenPredicateContext *>(ctx->children[0]); betweenCompareCtx != nullptr) {
		return parseBetweenCompareContext(betweenCompareCtx);
	}
	else if (auto *inCompareCtx = dynamic_cast<MySqlParser::InPredicateContext *>(ctx->children[0]); inCompareCtx != nullptr) {
		return parseInCompareContext(inCompareCtx);
	}

	throw SQLParseException("unsupport Predicate Expression");
}

Condition *MySQLSelectExprListener::parseBinaryCompareContext(MySqlParser::BinaryComparisonPredicateContext *ctx)
{
	std::string leftFieldName = "";
	std::string rightFieldName = "";
	MyVariant rightValue;
	FieldSchema *leftField = nullptr;
	FieldSchema *rightField = nullptr;

	std::string op = StrUtils::toUpper(ctx->children[1]->getText());
	auto child1 = ctx->children[0];
	auto child2 = ctx->children[2];
	if (isAggregateColumn(child1)) {
		return buildBinaryCompareCondition(
			static_cast<MySqlParser::AggregateFunctionCallContext *>(child1->children[0]->children[0]),
			child2, op);
	}
	else if (isAggregateColumn(child2)) {
		return buildBinaryCompareCondition(
			static_cast<MySqlParser::AggregateFunctionCallContext *>(child2->children[0]->children[0]),
			child1, op);
	}

	if (isColumnName(child1)) {
		return buildBinaryCompareCondition(
			static_cast<MySqlParser::FullColumnNameExpressionAtomContext *>(child1->children[0]), child2, op);
	}
	else if (isColumnName(child2)) {
		return buildBinaryCompareCondition(
			static_cast<MySqlParser::FullColumnNameExpressionAtomContext *>(child2->children[0]), child1, op);
	}

	return nullptr;
}

Condition *MySQLSelectExprListener::buildBinaryCompareCondition(
	MySqlParser::FullColumnNameExpressionAtomContext *left, antlr4::tree::ParseTree *right, 
	const std::string &op)
{
	std::string leftFieldName = left->getText();
	FieldSchema *leftField = findField(leftFieldName);
	if (!leftField) {
		std::cerr << "not exist field : " << leftFieldName << std::endl;
		return nullptr;
	}

	if (isColumnName(right)) {
		std::string rightFieldName = static_cast<antlr4::ParserRuleContext * >(right)->getText();
		FieldSchema *rightField = findField(rightFieldName);
		if (!rightField) {
			std::cerr << "not exist field : " << rightFieldName << std::endl;
			return nullptr;
		}

		return new FieldCondition(leftField, rightField, op);
	}
	else {
		int32_t curParamId = m_paramId++;
		return new ConstCondition(leftField, curParamId, curParamId, op);
	}
}

Condition *MySQLSelectExprListener::buildBinaryCompareCondition(
	MySqlParser::AggregateFunctionCallContext *left, antlr4::tree::ParseTree *right,
	const std::string &op)
{
	auto f = left->aggregateWindowedFunction();
	AggregateFunction leftFunc = functionOf(f->children[0]->getText());
	std::string leftFieldName = f->functionArg()->getText();
	FieldSchema *leftField = findField(leftFieldName);
	if (!leftField) {
		std::cerr << "not exist field : " << leftFieldName << std::endl;
		return nullptr;
	}

	if (isAggregateColumn(right)) {
		f = static_cast<MySqlParser::AggregateFunctionCallContext *>(
			right->children[0]->children[0])->aggregateWindowedFunction();
		AggregateFunction rightFunc = functionOf(f->children[0]->getText());
		std::string rightFieldName = f->functionArg()->getText();
		FieldSchema *rightField = findField(rightFieldName);
		if (!rightField) {
			std::cerr << "not exist field : " << rightFieldName << std::endl;
			return nullptr;
		}

		return new AggregateFieldCondition(leftField, rightField, op, leftFunc, rightFunc);
	}
	else if (isColumnName(right)) {
		std::string rightFieldName = static_cast<antlr4::RuleContext *>(right)->getText();
		FieldSchema *rightField = findField(rightFieldName);
		if (!rightField) {
			std::cerr << "not exist field : " << rightFieldName << std::endl;
			return nullptr;
		}

		return new AggregateFieldCondition(leftField, rightField, op, leftFunc);
	}
	else {
		return new AggregateConstCondition(leftField, leftFunc, m_paramId++, op);
	}
}

Condition *MySQLSelectExprListener::buildMultiConstCompareCondition(
	MySqlParser::FullColumnNameExpressionAtomContext *left, int32_t paramSize, const std::string &op)
{
	std::string leftFieldName = left->getText();
	FieldSchema *leftField = findField(leftFieldName);
	if (!leftField) {
		std::cerr << "not exist field : " << leftFieldName << std::endl;
		return nullptr;
	}

	m_paramId += paramSize;
	return new ConstCondition(leftField, m_paramId - paramSize, m_paramId - 1, op);
}

Condition *MySQLSelectExprListener::parseBetweenCompareContext(MySqlParser::BetweenPredicateContext *ctx)
{
	auto child = ctx->children[0];
	if (!isColumnName(child) || !isConstant(ctx->children[2]) || !isConstant(ctx->children[4])) {
		return nullptr;
	}
	
	return buildMultiConstCompareCondition(
		static_cast<MySqlParser::FullColumnNameExpressionAtomContext *>(child->children[0]), 2, "BETWEEN");
}

Condition *MySQLSelectExprListener::parseInCompareContext(MySqlParser::InPredicateContext *ctx)
{
	auto child = ctx->children[0];
	if (!isColumnName(child)) {
		return nullptr;
	}

	MySqlParser::ExpressionsContext *expr = ctx->getRuleContext<MySqlParser::ExpressionsContext>(0);
	if (!expr) {
		return nullptr;
	}

	for (int i = 0; i < expr->children.size(); i += 2) {
		if (!isConstant(expr->children[i])) {
			return nullptr;
		}
	}

	return buildMultiConstCompareCondition(
		static_cast<MySqlParser::FullColumnNameExpressionAtomContext *>(child->children[0]), 
		expr->children.size() / 2 + 1, "IN");
}

void MySQLSelectExprListener::parseSelectColumnContext(MySqlParser::SelectColumnElementContext *ctx)
{
	std::string colName = ctx->fullColumnName()->getText();
	m_selectColumns.push_back(colName);
	if (ctx->uid()) {
		m_sqlColStrMaps[colName] = ctx->uid()->getText();
	}
}

void MySQLSelectExprListener::parseSelectColumns()
{
	for (int i = 0; i < m_selectColumns.size(); ++i) {
		if (m_selectColumns[i] == ALL_COLUMN_WORD) {
			SQLNormalTableSchema *owner = m_tableSchemas[0].schema;
			for (int j = 0; j < owner->fieldCount(); ++j) {
				m_selectFields.push_back(owner->field(j));
			}
			continue;
		}

		FieldSchema *field = findField(m_selectColumns[i]);
		if (field) {
			m_selectFields.push_back(field);
			auto m = m_sqlColStrMaps.find(m_selectColumns[i]);
			if (m != m_sqlColStrMaps.end()) {
				m_sqlColMaps[field] = m->second;
			}
		}
	}
}

void MySQLSelectExprListener::parseAggregateColumns()
{
	if (m_aggregateInfoStrs.empty()) {
		return;
	}

	for (int i = m_aggregateInfoStrs.size() - 1; i >= 0; --i) {
		auto &infoStr = m_aggregateInfoStrs[i];
		FieldSchema *field = findField(std::get<1>(infoStr));
		if (field) {
			AggregateFieldInfo info;
			info.aggregateFunc = functionOf(std::get<0>(infoStr));
			info.field = field;
			info.name = std::get<2>(infoStr);
			m_aggregateFieldInfo.push_back(info);
		}
	}
}

void MySQLSelectExprListener::parseSelectFunctionContext(MySqlParser::SelectFunctionElementContext *ctx)
{
	auto funcCtx = ctx->getRuleContext<MySqlParser::AggregateFunctionCallContext>(0);
	if (funcCtx) {
		auto f = funcCtx->aggregateWindowedFunction();
		std::string funcName = f->children[0]->getText();
		std::string columnName = f->functionArg()->getText();
		m_aggregateInfoStrs.push_back(std::make_tuple(funcName, columnName, 
			ctx->uid() ? ctx->uid()->getText() : ""));
	}
	else {
		throw SQLParseException("unsupport function");
	}
}

FieldSchema *MySQLSelectExprListener::findField(const std::string &name)
{
	std::string tableName = "";
	std::string colName = name;
	int pos = name.find('.');
	if (pos > 0) {
		tableName = name.substr(0, pos);
		colName = name.substr(pos + 1);
	}

	for (int i = 0; i < m_tableSchemas.size(); ++i) {
		if (!tableName.empty() && m_tableSchemas[i].name != tableName) {
			continue;
		}

		SQLNormalTableSchema *tableSchema = m_tableSchemas[i].schema;
		FieldSchema *field = tableSchema->findField(colName);
		if (field) {
			return field;
		}
	}
	return nullptr;
}

void MySQLSelectExprListener::dfs_condition(Condition *cond, const ConditionIteration &iteration)
{
	if (cond->kind() == ConditionKind::ckBinary) {
		dfs_condition(static_cast<BinaryCondition *>(cond)->left(), iteration);
		dfs_condition(static_cast<BinaryCondition *>(cond)->right(), iteration);
	}
	else {
		iteration(static_cast<SimpleCondition *>(cond));
	}
}

void MySQLSelectExprListener::forEachCondition(std::shared_ptr<Condition> conditionObj, 
	const ConditionIteration &iteration)
{
	if (!conditionObj) {
		return;
	}

	dfs_condition(conditionObj.get(), iteration);
}

std::shared_ptr<Condition> MySQLSelectExprListener::condition() const
{
	return m_where;
}

std::shared_ptr<Condition> MySQLSelectExprListener::having() const
{
	return m_having;
}

SQLJoinType MySQLSelectExprListener::joinType() const
{
	return m_joinType;
}

int MySQLSelectExprListener::tableCount() const
{
	return m_tableSchemas.size();
}

SQLNormalTableSchema *MySQLSelectExprListener::tableSchema(int index) const
{
	return m_tableSchemas.at(index).schema;
}

std::string MySQLSelectExprListener::tableName(int index) const
{
	return m_tableSchemas.at(index).name;
}

std::string MySQLSelectExprListener::getSqlColumnName(FieldSchema *fd)
{
	auto r = m_sqlColMaps.find(fd);
	if (r != m_sqlColMaps.end()) {
		return r->second;
	}

	return "";
}

int MySQLSelectExprListener::selectFieldCount()
{
	return m_selectFields.size();
}

FieldSchema *MySQLSelectExprListener::selectField(int index) const
{
	return m_selectFields.at(index);
}

int MySQLSelectExprListener::orderByFieldCount()
{
	return m_orderByFields.size();
}

const OrderFieldInfo &MySQLSelectExprListener::orderByField(int index) const
{
	return m_orderByFields.at(index);
}

int MySQLSelectExprListener::groupByFieldCount()
{
	return m_groupByFields.size();
}

FieldSchema *MySQLSelectExprListener::groupByField(int index) const
{
	return m_groupByFields.at(index);
}

int MySQLSelectExprListener::aggregateFieldCount()
{
	return m_aggregateFieldInfo.size();
}

const AggregateFieldInfo &MySQLSelectExprListener::aggregateField(int index) const
{
	return m_aggregateFieldInfo.at(index);
}

MySQLUpdateExprListener::MySQLUpdateExprListener(SQLContext *context) :
	MySQLExprListener(context),
	m_tableSchema(nullptr)
{
}

MySQLUpdateExprListener::~MySQLUpdateExprListener()
{
}

void MySQLUpdateExprListener::enterTableName(MySqlParser::TableNameContext * ctx)
{
	m_tableSchema = m_context->findTable(ctx->getText());
}

void MySQLUpdateExprListener::enterUpdatedElement(MySqlParser::UpdatedElementContext *ctx)
{
	auto colCtx = dynamic_cast<MySqlParser::FullColumnNameContext *>(ctx->children[0]);
	FieldSchema *field = m_tableSchema->findField(colCtx->getText());
	m_updateFields.push_back(field);

	auto valueExprCtx = dynamic_cast<antlr4::ParserRuleContext *>(ctx->children[2]);
	if (valueExprCtx->getText() != "'?'") {
		throw SQLParseException("Update Param Must Be ?");
	}
}

void MySQLUpdateExprListener::enterSingleUpdateStatement(MySqlParser::SingleUpdateStatementContext *ctx)
{
}

int MySQLUpdateExprListener::updateFieldCount() const
{
	return m_updateFields.size();
}

FieldSchema *MySQLUpdateExprListener::updateField(int index) const
{
	return m_updateFields.at(index);
}

SQLNormalTableSchema *MySQLUpdateExprListener::tableSchema(int index) const
{
	return m_tableSchema;
}

MySQLInsertExprListener::MySQLInsertExprListener(SQLContext *context) :
	MySQLExprListener(context),
	m_curFieldIndex(0),
	m_valid(true)
{
}

MySQLInsertExprListener::~MySQLInsertExprListener()
{
}

void MySQLInsertExprListener::enterTableName(MySqlParser::TableNameContext *ctx)
{
	m_tableSchema = m_context->findTable(ctx->getText());
}

void MySQLInsertExprListener::enterFullColumnNameList(MySqlParser::FullColumnNameListContext *ctx)
{
	for (int i = 0; i < ctx->children.size(); ++i) {
		auto ch = ctx->children[i];
		if (auto *columnCtx = dynamic_cast<MySqlParser::FullColumnNameContext *>(ch); columnCtx != nullptr) {
			std::string fieldName = columnCtx->getText();
			m_fields.push_back(m_tableSchema->findField(fieldName));
		}
	}
}

void MySQLInsertExprListener::enterExpressionOrDefault(MySqlParser::ExpressionOrDefaultContext * ctx)
{
	if (m_curFieldIndex >= m_fields.size()) {
		m_curFieldIndex = 0;
	}

	try {
		m_values.push_back(getFieldValue(ctx->getText(), m_fields[m_curFieldIndex++]->dataType()));
	} 
	catch (...) {
		m_valid = false;
	}
}

int MySQLInsertExprListener::recordCount() const
{
	return m_values.size() / m_fields.size();
}

int MySQLInsertExprListener::insertFieldCount() const
{
	return m_fields.size();
}

FieldSchema *MySQLInsertExprListener::insertField(int index) const
{
	return m_fields.at(index);
}

const MyVariant &MySQLInsertExprListener::insertValue(int index) const
{
	return m_values.at(index);
}

SQLNormalTableSchema *MySQLInsertExprListener::tableSchema(int index) const
{
	return m_tableSchema;
}

bool MySQLInsertExprListener::valid() const
{
	return m_valid;
}

MySQLDeleteExprListener::MySQLDeleteExprListener(SQLContext *context) :
	MySQLExprListener(context),
	m_tableSchema(nullptr)
{
}

MySQLDeleteExprListener::~MySQLDeleteExprListener()
{
}

void MySQLDeleteExprListener::enterTableName(MySqlParser::TableNameContext *ctx)
{
	m_tableSchema = m_context->findTable(ctx->getText());
}

void MySQLDeleteExprListener::enterSingleDeleteStatement(MySqlParser::SingleDeleteStatementContext *ctx)
{
}

SQLNormalTableSchema *MySQLDeleteExprListener::tableSchema(int index) const
{
	return m_tableSchema;
}

MySQLExprListener::MySQLExprListener(SQLContext *context) :
	MySqlParserBaseListener(),
	m_context(context)
{
}

MySQLExprListener::~MySQLExprListener()
{
}
