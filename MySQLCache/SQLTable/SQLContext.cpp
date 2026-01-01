#include "SQLContext.h"
#include "SQLTableSchema.h"
#include "SQLTable.h"
#include "antlr4-runtime.h"
#include "MySqlParser.h"
#include "MySqlLexer.h"
#include "MySQLExprListener.h"
#include "Common.h"
#include "TaskQueue.h"
#include "Task.h"
#include "Consts.h"
#include "CacheMonitor.h"
#include "StrUtils.h"
#include "SQLTableContainer.h"
#include "MemoryManager.h"
#include "SQLConnectorException.h"
#include "SQLParseException.h"
#include <chrono>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <unordered_set>
#include <atomic>

using namespace antlr4;
using namespace std;

const char *WHERE_TOKEN = "where";
const char *ON_TOKEN = "on";
const char *HAVING_TOKEN = "having";
const char *GROUP_TOKEN = "group";
const char *ORDER_TOKEN = "order";

const std::string COLUMN_NAME_SEPRATOR = "___";

SQLContext *g_context = nullptr;

const int8_t MOST_BUSY_GAP = 10;
const uint32_t EXT_INFO_LEN = 9;
const int32_t MAX_READ_TASK_TIME = 2;  //minute
const int32_t MAX_WRITE_TASK_TIME = 4;  //minute


void threadFunc(SQLContext *context, int thIndex, int threadId)
{
	while (Task* task = context->fetchTask(thIndex, threadId))
	{
		context->executeTask(task, thIndex);
	}
}

SQLContext::SQLContext(bool readMode, int threadCount, const std::string &serverAddr,
	const std::string &sqlType, bool enableMonitor) :
	m_readMode(readMode),
	m_threadCnt(threadCount),
	m_sqlType(sqlType),
	m_enableMonitor(enableMonitor),
	m_sendBuff(nullptr),
	m_buffer(nullptr),
	m_bufferLen(0),
	m_lockIndex(0)
{
	initialize(serverAddr);
}

SQLContext::~SQLContext()
{
	for (int i = 0; i < m_threadCnt; ++i) {
		m_threads[i]->join();
		delete m_threads[i];

		delete m_taskQueues[i];
		delete m_waitTaskLocks[i];
		delete m_waitTaskConds[i];
	}

	if (m_readMode) {
		for (int i = 0; i < m_threadCnt; ++i) {
			delete m_graphs[i];
			FOR_EACH(j, m_cacheTableSchemas[i]) {
				delete j->second->schema;
				delete j->second;
			}
			delete m_updateCacheLocks[i];
		}

		delete[] m_cacheTableSchemas;
	}

	FOR_EACH(i, m_connectors) {
		delete *i;
	}

	FOR_EACH(i, m_tableSchemas) {
		delete i->second;
	}
}

SQLContext *SQLContext::instance()
{
	return g_context;
}

void SQLContext::setInstance(SQLContext *ctx)
{
	g_context = ctx;
}

bool SQLContext::readMode() const
{
	return m_readMode;
}

void SQLContext::initialize(const std::string& serverAddr)
{
	for (int i = 0; i < m_threadCnt; ++i) {
		m_taskQueues.push_back(new TaskQueue());
		m_taskQueues[i]->setThreadId(i);
		if (m_readMode) {
			MemoryManager::instantce(i).setTaskQueue(m_taskQueues[i]);
		}
		
		m_waitTaskLocks.push_back(new mutex());
		m_waitTaskConds.push_back(new condition_variable());

		if (m_readMode) {
			m_updateCacheLocks.push_back(new mutex());
		}

	}

	if (m_readMode) {
		for (int i = 0; i < m_threadCnt; ++i) {
			SQLGraph *graph = new SQLGraph(i);
			m_graphs.push_back(graph);
		}

		m_cacheTableSchemas = new TableSchemaHash[m_threadCnt];
	}
	
	for (int i = 0; i < m_threadCnt; ++i) {
		SQLConnector *connector = SQLConnectorFactory::createConnector(m_sqlType);
		m_connectors.push_back(connector);
	}
	connect(serverAddr);

	for (int i = 0; i < m_threadCnt; ++i) {
		thread *th = new thread(threadFunc, this, i, i);
		m_threads.push_back(th);
	}

	if (m_enableMonitor) {
		CacheMonitor::instance()->init(m_threadCnt);
	}
}

void SQLContext::free()
{
	for (int i = 0; i < m_threadCnt; ++i) {
		m_connectors[i]->disconnect();
	}
}

void SQLContext::test()
{
	//std::string sqlStr = "SELECT b.description, a.name AS aName FROM teacher a JOIN subject b ON a.subjectID = b.subjectID;";
	std::string sqlStr = "SELECT * FROM student WHERE name <> '' ORDER BY name, score DESC;";
	//std::string sqlStr = "SELECT Sum(id) AS IDCount FROM student GROUP BY name HAVING name = 'AA' AND sum(score) > 0;";
	//std::string sqlStr = "SELECT *  FROM student WHERE score IN (100)";
	ANTLRInputStream input(sqlStr);
	MySqlLexer lexer(&input);
	CommonTokenStream tokens(&lexer);
	MySqlParser parser(&tokens);
	MySQLSelectExprListener listener(this, sqlStr);
	tree::ParseTreeWalker::DEFAULT.walk(&listener, parser.sqlStatements());
	//m_connectors[0]->select("SELECT * FROM student");
}

SQLNormalTableSchema *SQLContext::findTable(const std::string &name)
{
	auto i = m_tableSchemas.find(name);
	if (i != m_tableSchemas.end()) {
		return i->second;
	}

	return nullptr;
}

SQLNormalTableSchema *SQLContext::addTable(const std::string &name)
{
	if (m_tableSchemas.find(name) == m_tableSchemas.end()) {
		SQLNormalTableSchema *newTable = new SQLNormalTableSchema();
		newTable->setName(name);
		m_tableSchemas[name] = newTable;
	}

	return m_tableSchemas[name];
}

SQLTable *SQLContext::addCacheTable(SQLTableSchema *schema, int thIndex, uint32_t &tableID)
{
	SQLTable *table = nullptr;
	tableID = SQLTableContainer::instance(thIndex)->newTable(schema, &table);
	if (table) {
		table->setThreadIndex(thIndex);
	}
	return table;
}

SQLTable *SQLContext::selectCacheTable(const std::string &sql, MyVariants &params, 
	std::vector<int8_t> &paramTypes, int thIndex)
{
	SQLTable *cacheTable = nullptr;
	SQLTableSchemaInfo *schemaInfo = nullptr;
	SQLSchemaVertex *schemaVtx = nullptr;
	auto i = m_cacheTableSchemas[thIndex].find(sql);
	if (i != m_cacheTableSchemas[thIndex].end()) {
		schemaInfo = i->second;
	}

	if (schemaInfo) {
		schemaVtx = static_cast<SQLSchemaVertex *>(
			m_graphs[thIndex]->findVertex(reinterpret_cast<intptr_t>(schemaInfo->schema)));
		cacheTable = schemaVtx->findTable(params, thIndex);
	}

	if (cacheTable) {
		if (m_enableMonitor) {
			CacheMonitor::instance()->writeHit(sql, thIndex);
		}
		return cacheTable;
	}

	string addSql;
	if (!schemaInfo) {
		schemaInfo = createCacheTableSchema(sql, thIndex);
	}
	// grammar error or uncacheable SELECT statement
	if (!schemaInfo) {
		return nullptr;
	}

	if (!schemaVtx) {
		schemaVtx = static_cast<SQLSchemaVertex *>(
			m_graphs[thIndex]->findVertex(reinterpret_cast<intptr_t>(schemaInfo->schema)));
	}

	uint32_t tableID = 0;
	cacheTable = addCacheTable(schemaInfo->schema, thIndex, tableID);
	cacheTable->params() = params;
	schemaVtx->addTable(cacheTable, tableID, thIndex);

	string realSql = sql;
	if (!schemaInfo->addSql.empty()) {
		// 7 == length of 'SELECT '
		realSql = StrUtils::join(realSql.substr(0, 7), schemaInfo->addSql, realSql.substr(7));
	} 
	
	m_connectors[thIndex]->select(realSql, params, paramTypes, cacheTable);
	return cacheTable;
}

void SQLContext::directQuery(const std::string &sql, MyVariants &params,
	std::vector<int8_t>& paramTypes, int thIndex, WriteBuffer *buffer)
{
	m_connectors[thIndex]->select(sql, buffer, params, paramTypes);
}

SQLContext::SQLTableSchemaInfo *SQLContext::createCacheTableSchema(const std::string &sql, 
	int thIndex)
{	
	string addSql;
	SQLTableSchema *tableSchema = doCreateCacheTableSchema(sql, addSql, thIndex);
	if (tableSchema) {
		SQLTableSchemaInfo *info = new SQLTableSchemaInfo();
		info->schema = tableSchema;
		info->addSql = addSql;
		m_cacheTableSchemas[thIndex][sql] = info;
		return info;
	}

	return nullptr;
}

void SQLContext::parseSQLInfo(SQLInfo &info)
{
	std::vector<int> replacePos;
	std::vector<int> newLinePos;
	std::string token;
	int tokenStart = -1;
	int tokenLen = 0;
	bool inWhere = false;
	for (int i = 0; i < info.sql.size(); ++i) {
		if (info.sql[i] == '\'' || info.sql[i] == '"' || info.sql[i] == '`') {
			char quoteChar = info.sql[i];
			tokenStart = i;
			tokenLen = 1;
			for (++i; i < info.sql.size(); ++i) {
				++tokenLen;
				if (info.sql[i] == quoteChar) {
					break;
				}
			}

			if (inWhere) {
				replacePos.push_back(tokenStart);
				replacePos.push_back(tokenLen);
				info.params.add(info.sql.substr(tokenStart + 1, tokenLen - 2));
			}

			tokenStart = -1;
			tokenLen = 0;
			continue;
		}

		if (info.sql[i] == ' ' || info.sql[i] == '\r' || info.sql[i] == '\n' || info.sql[i] == '\t') {
			if (info.sql[i] == '\n') {
				newLinePos.push_back(i);
			}

			if (tokenLen > 0) {
				token = info.sql.substr(tokenStart, tokenLen);
				if (strcmp(token.c_str(), WHERE_TOKEN) == 0 ||
					strcmp(token.c_str(), ON_TOKEN) == 0) {
					inWhere = true;
				}
				else if (strcmp(token.c_str(), ORDER_TOKEN) == 0 ||
					strcmp(token.c_str(), GROUP_TOKEN) == 0) {
					inWhere = false;
				}
				else {
					if (inWhere && doConstToken(info.params, token)) {
						replacePos.push_back(tokenStart);
						replacePos.push_back(tokenLen);
					}
				}
				tokenStart = -1;
				tokenLen = 0;
			}
			continue;
		}

		if (tokenStart == -1) {
			tokenStart = i;
		}
		++tokenLen;
	}

	if (tokenLen > 0) {
		token = info.sql.substr(tokenStart, tokenLen);
		if (inWhere && doConstToken(info.params, token)) {
			replacePos.push_back(tokenStart);
			replacePos.push_back(tokenLen);
		}
	}

	info.schema = info.sql;
	// replace
	for (int i = replacePos.size() - 1; i >= 0; i -= 2) {
		int pos = replacePos[i - 1];
		int len = replacePos[i];
		info.schema = info.schema.substr(0, pos).append(info.schema.substr(pos + len));
	}

	for (int i = newLinePos.size() - 1; i >= 0; --i) {
		info.sql = info.sql.substr(0, newLinePos[i]).append(" ")
			.append(info.sql.substr(newLinePos[i] + 1));
	}
}

bool SQLContext::doConstToken(MyVariants &params, const std::string &token)
{
	if (token[0] >= '0' && token[0] <= '9') {
		params.add(atof(token.c_str()));
		return true;
	}
	else if (strcmp(token.c_str(), "true") == 0) {
		params.add(true);
		return true;
	}
	else if (strcmp(token.c_str(), "false") == 0) {
		params.add(false);
		return true;
	}

	return false;
}

bool SQLContext::isChinese(unsigned char c)
{
	return c > 127;
}

SQLTableSchema *SQLContext::doCreateCacheTableSchema(const std::string &sqlStr,
	std::string &addSql, int thIndex)
{
	ANTLRInputStream input(formatAntlrSql(sqlStr));
	MySqlLexer lexer(&input);
	CommonTokenStream tokens(&lexer);
	MySqlParser parser(&tokens);

	MySQLSelectExprListener listener(this, sqlStr);
	try {
		tree::ParseTreeWalker::DEFAULT.walk(&listener, parser.sqlStatements());
	}
	catch (SQLParseException &e) {
		std::cerr << e.what() << std::endl;
		return nullptr;
	}
	
	if (listener.groupByFieldCount() > 0) {
		return createGroupByTableSchema(sqlStr, &listener, thIndex);
	}
	else if (listener.joinType() == SQLJoinType::sjtNull) {
		return createNormalTableSchema(sqlStr, addSql, &listener, thIndex);
	}
	else {
		return createJoinTableSchema(sqlStr, addSql, &listener, thIndex);
	}
}

SQLTableSchema *SQLContext::createGroupByTableSchema(const std::string &sqlStr, 
	MySQLSelectExprListener *listener, int thIndex)
{
	SQLNormalTableSchema *result = new SQLNormalTableSchema();
	result->setName(listener->tableName(0));
	result->setGroupBy(true);
	SQLSchemaVertex *schemaVtx = static_cast<SQLSchemaVertex *>(m_graphs[thIndex]->addVertex(result));

	bool existPK = false;
	for (int i = 0; i < listener->selectFieldCount(); ++i) {
		FieldSchema *field = listener->selectField(i);
		std::string columnName = listener->getSqlColumnName(field);
		std::string fieldName = columnName.empty() ? field->name() : columnName;
		auto resultField = result->addField(fieldName, field->dataType());
		if (resultField) {
			resultField->setQuery(true);
			addFieldVtx(m_graphs[thIndex], field, schemaVtx);
		}
	}

	for (int i = 0; i < listener->aggregateFieldCount(); ++i) {
		auto info = listener->aggregateField(i);
		auto resultField = result->addField(info.name,
			info.aggregateFunc == AggregateFunction::gfCount ? DataType::dtInt : info.field->dataType());
		if (resultField) {
			resultField->setQuery(true);
			addFieldVtx(m_graphs[thIndex], info.field, schemaVtx);
		}
	}

	auto conditionIterFunc = [&](SimpleCondition *conditionObj) {
		if (conditionObj->kind() == ConditionKind::ckConst) {
			FieldSchema *field = static_cast<ConstCondition *>(conditionObj)->leftField();
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false, true);
		}
		else if (conditionObj->kind() == ConditionKind::ckField) {
			FieldSchema *field = static_cast<FieldCondition *>(conditionObj)->leftField();
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false, true);

			field = static_cast<FieldCondition *>(conditionObj)->rightField();
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false, true);
		}
		else if (conditionObj->kind() == ConditionKind::ckAggregateConst) {
			AggregateConstCondition *cond = static_cast<AggregateConstCondition *>(conditionObj);
			FieldSchema *field = cond->leftField();
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false);
		}
		else if (conditionObj->kind() == ConditionKind::ckAggregateField) {
			AggregateFieldCondition *cond = static_cast<AggregateFieldCondition *>(conditionObj);
			FieldSchema *field = cond->leftField();
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false);

			field = static_cast<AggregateFieldCondition *>(conditionObj)->rightField();
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false);
		}
	};

	listener->forEachCondition(listener->condition(), conditionIterFunc);
	listener->forEachCondition(listener->having(), conditionIterFunc);

	for (int i = 0; i < listener->orderByFieldCount(); ++i) {
		OrderFieldInfo fieldInfo = listener->orderByField(i);
		addFieldVtx(m_graphs[thIndex], fieldInfo.field, schemaVtx, false, false, true);
	}

	for (int i = 0; i < listener->groupByFieldCount(); ++i) {
		FieldSchema *field = listener->groupByField(i);
		addFieldVtx(m_graphs[thIndex], field, schemaVtx, false);
	}

	result->compile();
	return result;
}

SQLTableSchema *SQLContext::createNormalTableSchema(const std::string &sqlStr, std::string &addSql,
	MySQLSelectExprListener *listener, int thIndex)
{
	SQLNormalTableSchema *result = new SQLNormalTableSchema();
	result->setName(listener->tableName(0));
	SQLSchemaVertex *schemaVtx = static_cast<SQLSchemaVertex *>(m_graphs[thIndex]->addVertex(result));
	schemaVtx->setCondition(listener->condition());

	bool existPK = false;
	for (int i = 0; i < listener->selectFieldCount(); ++i) {
		FieldSchema *field = listener->selectField(i);
		auto resultField = result->addField(*field);
		if (resultField) {
			resultField->setQuery(true);
			addFieldVtx(m_graphs[thIndex], field, schemaVtx);

			std::string columnName = listener->getSqlColumnName(field);
			if (!columnName.empty()) {
				result->addColumnMap(resultField->name(), columnName);
			}
		}
	}

	auto conditionIterFunc = [&](SimpleCondition *conditionObj) {
		if (conditionObj->kind() == ConditionKind::ckConst) {
			FieldSchema *field = static_cast<ConstCondition *>(conditionObj)->leftField();
			result->addField(*field);
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false, true);
		}
		else if (conditionObj->kind() == ConditionKind::ckField) {
			FieldSchema *field = static_cast<FieldCondition *>(conditionObj)->leftField();
			result->addField(*field);
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false, true);

			field = static_cast<FieldCondition *>(conditionObj)->rightField();
			result->addField(*field);
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false, true);
		}
	};

	listener->forEachCondition(schemaVtx->condition(), conditionIterFunc);

	for (int i = 0; i < listener->orderByFieldCount(); ++i) {
		OrderFieldInfo fieldInfo = listener->orderByField(i);
		auto resultField = result->addField(*(fieldInfo.field));
		if (resultField) {
			addFieldVtx(m_graphs[thIndex], fieldInfo.field, schemaVtx, false, false, true);
			fieldInfo.field = resultField;
			result->addOrderField(fieldInfo);
		}
	}

	for (int i = 0; i < result->fieldCount(); ++i) {
		FieldSchema *field = result->field(i);
		if (!field->isQuery()) {
			StrUtils::append(addSql, field->name(), ",");
		}
	}

	if (result->primaryKey() == nullptr) {
		FieldSchema *fd = listener->tableSchema(0)->primaryKey();
		result->addField(*fd);
		StrUtils::append(addSql, fd->name(), ",");
	}

	result->compile();
	return result;
}

SQLTableSchema *SQLContext::createJoinTableSchema(const std::string &sqlStr, std::string &addSql,
	MySQLSelectExprListener *listener, int thIndex)
{
	SQLJoinTableSchema *result = new SQLJoinTableSchema();
	result->left()->setName(listener->tableSchema(0)->name());
	result->right()->setName(listener->tableSchema(1)->name());

	SQLSchemaVertex *schemaVtx = static_cast<SQLSchemaVertex *>(m_graphs[thIndex]->addVertex(result));
	schemaVtx->setCondition(listener->condition());
	for (int i = 0; i < listener->selectFieldCount(); ++i) {
		FieldSchema *field = listener->selectField(i);
		auto resultField = result->table(field->tableName())->addField(*field);
		if (resultField) {
			resultField->setQuery(true);
			addFieldVtx(m_graphs[thIndex], field, schemaVtx);

			std::string columnName = listener->getSqlColumnName(field);
			if (!columnName.empty()) {
				result->table(field->tableName())->addColumnMap(resultField->name(), columnName);
			}
		}
	}

	auto conditionIterFunc = [&](SimpleCondition *conditionObj) {
		if (conditionObj->kind() == ConditionKind::ckConst) {
			FieldSchema *field = static_cast<ConstCondition *>(conditionObj)->leftField();
			result->table(field->tableName())->addField(*field);
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false, true);
		}
		else if (conditionObj->kind() == ConditionKind::ckField) {
			FieldSchema *field = static_cast<FieldCondition *>(conditionObj)->leftField();
			result->table(field->tableName())->addField(*field);
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false, true);

			field = static_cast<FieldCondition *>(conditionObj)->rightField();
			result->table(field->tableName())->addField(*field);
			addFieldVtx(m_graphs[thIndex], field, schemaVtx, false, true);
		}
	};

	listener->forEachCondition(schemaVtx->condition(), conditionIterFunc);

	for (int i = 0; i < listener->orderByFieldCount(); ++i) {
		OrderFieldInfo fieldInfo = listener->orderByField(i);
		auto resultField = result->table(fieldInfo.field->tableName())->addField(*(fieldInfo.field));
		if (resultField) {
			addFieldVtx(m_graphs[thIndex], fieldInfo.field, schemaVtx, false, false, true);
			fieldInfo.field = resultField;
			result->addOrderField(fieldInfo);
		}
	}

	std::string currentTableName = listener->tableName(0);
	for (int i = 0; i < result->left()->fieldCount(); ++i) {
		FieldSchema *fd = result->left()->field(i);
		if (!fd->isQuery()) {
			std::string columnName = std::string(result->left()->name())
				.append(COLUMN_NAME_SEPRATOR).append(fd->name());
			StrUtils::append(addSql, currentTableName, ".", fd->name(), " ", columnName, ",");
			result->left()->addColumnMap(fd->name(), columnName);
		}
	}

	if (result->left()->primaryKey() == nullptr) {
		FieldSchema *fd = listener->tableSchema(0)->primaryKey();
		result->left()->addField(*fd);
		std::string columnName = std::string(result->left()->name())
			.append(COLUMN_NAME_SEPRATOR).append(fd->name());
		StrUtils::append(addSql, currentTableName, ".", fd->name(), " ", columnName, ",");
		result->left()->addColumnMap(fd->name(), columnName);
	}

	currentTableName = listener->tableName(1);
	for (int i = 0; i < result->right()->fieldCount(); ++i) {
		FieldSchema *fd = result->right()->field(i);
		if (!fd->isQuery()) {
			std::string columnName = std::string(result->right()->name())
				.append(COLUMN_NAME_SEPRATOR).append(fd->name());
			StrUtils::append(addSql, currentTableName, ".", fd->name(), " ", columnName, ",");
			result->right()->addColumnMap(fd->name(), columnName);
		}
	}

	if (result->right()->primaryKey() == nullptr) {
		FieldSchema *fd = listener->tableSchema(1)->primaryKey();
		result->right()->addField(*fd);
		std::string columnName = std::string(result->right()->name())
			.append(COLUMN_NAME_SEPRATOR).append(fd->name());
		StrUtils::append(addSql, currentTableName, ".", fd->name(), " ", columnName, ",");
		result->right()->addColumnMap(fd->name(), columnName);
	}

	result->compile();
	return result;
}

void SQLContext::addFieldVtx(SQLGraph *graph, FieldSchema *field, SQLVertex *schemaVtx, 
	bool isQuery, bool isWhere, bool isOrder)
{
	SQLFieldVertex *vtx = static_cast<SQLFieldVertex *>(graph->addVertex(field));
	SQLEdge *edge = vtx->appendEdge(schemaVtx);
	edge->setQuery(isQuery);
	edge->setWhere(isWhere);
	edge->setOrder(isOrder);
}
// 单线程执行
void SQLContext::doSelect(SelectTaskData *task, int thIndex)
{
	InputStream in(task->sqlBytes);
	std::string sql = in.readText();
	MyVariants params;
	vector<int8_t> paramTypes;
	readParams(in, params, paramTypes);

	SQLTable *table = selectCacheTable(sql, params, paramTypes, thIndex);
	if (table) {
		table->save(task->buffer);
	}
	else {
		directQuery(sql, params, paramTypes, thIndex, task->buffer);
	}
}
void SQLContext::doWrite(Task *task, int thIndex)
{
	auto data = reinterpret_cast<WriteTaskData *>(task->data);
	if (m_readMode) {
		uint32_t offset = data->buffer->writePos();
		data->buffer->writeUByte(thIndex);
		data->buffer->writeULong(reinterpret_cast<uint64_t>(data));
		data->buffer->writeBytes(data->sqlBytes);
		InputStream in(data->sqlBytes);
		string dd = in.readText();
		if (!send(data->buffer, offset)) {
			data->errorCode = SQLCacheErrorCode::scecWriteServerError;
			setTaskFinish(data, thIndex);
		}
		return;
	}

	switch (task->type) {
	case TaskType::ttInsert:
		doInsert(data, thIndex);
		break;
	case TaskType::ttDelete:
		doRemove(data, thIndex);
		break;
	case TaskType::ttUpdate:
		doUpdate(data, thIndex);
		break;
	case TaskType::ttTransaction:
		doTransaction(data, thIndex);
		break;
	default:
		break;
	}
	setTaskFinish(data, thIndex);
}
// single thread execute
void SQLContext::doInsert(WriteTaskData *task, int thIndex)
{
	InputStream in(task->sqlBytes);
	std::string sql = in.readText();
	ANTLRInputStream input(formatAntlrSql(sql));
	MySqlLexer lexer(&input);
	CommonTokenStream tokens(&lexer);
	MySqlParser parser(&tokens);
	auto sqlStat = parser.sqlStatements();
	if (parser.getNumberOfSyntaxErrors() > 0) {
		task->errorCode = SQLCacheErrorCode::scecInvalidSql;
		return;
	}

	MySQLInsertExprListener listener(this);
	try {
		tree::ParseTreeWalker::DEFAULT.walk(&listener, sqlStat);
	}
	catch (...) {
		task->errorCode = SQLCacheErrorCode::scecInvalidCacheSql;
	}
	
	int64_t newID = -1;
	MyVariants params;
	vector<int8_t> paramTypes;
	readParams(in, params, paramTypes);
	try {
		newID = m_connectors[thIndex]->insert(sql, params, paramTypes);
	}
	catch (SQLConnectorException &) {
		std::cerr << sql << " execute error" << std::endl;
		task->errorCode = SQLCacheErrorCode::scecSqlFail;
		return;
	}
	
	task->updateCount = listener.recordCount();
	if (task->errorCode == SQLCacheErrorCode::scecInvalidCacheSql) {
		// can't reconize insert values in new records，so all caches with the table refresh
		flushAllTableCache(&listener, task, thIndex);
	}
	else {
		SQLNormalTable resultTable(listener.tableSchema());
		resultTable.setThreadIndex(thIndex);
		int paramIndex = 0;
		for (int i = 0; i < listener.recordCount(); ++i) {
			SQLRecord *insertRec = resultTable.newRecord();
			for (int j = 0; j < listener.tableSchema()->fieldCount(); ++j) {
				FieldSchema* fd = listener.tableSchema()->field(j);
				if (fd->isPrimaryKey()) {
					continue;
				}

				insertRec->setValue(fd->name(), nullptr);
			}
			insertRec->setValue(resultTable.primaryKey()->name(), newID++);
			resultTable.append(insertRec);
			for (int j = 0; j < listener.insertFieldCount(); ++j) {
				const std::string& fieldName = listener.insertField(j)->name();
				const MyVariant &value = listener.insertValue(i * listener.insertFieldCount() + j);
				if (value == "?") {
					insertRec->setValue(fieldName, params.variant(paramIndex++));
				}
				else {
					insertRec->setValue(fieldName, value);
				}
			}
		}

		writeUpdateTable(resultTable, UpdateOperation::umInsert, task->buffer);
	}	
}
// single thread execute
void SQLContext::doRemove(WriteTaskData *task, int thIndex)
{
	InputStream in(task->sqlBytes);
	std::string sql = in.readText();
	ANTLRInputStream input(formatAntlrSql(sql));
	MySqlLexer lexer(&input);
	CommonTokenStream tokens(&lexer);
	MySqlParser parser(&tokens);
	auto sqlStat = parser.sqlStatements();
	if (parser.getNumberOfSyntaxErrors() > 0) {
		task->errorCode = SQLCacheErrorCode::scecInvalidSql;
		return;
	}

	MySQLDeleteExprListener listener(this);
	try {
		tree::ParseTreeWalker::DEFAULT.walk(&listener, sqlStat);
	}
	catch (...) {
		task->errorCode = SQLCacheErrorCode::scecInvalidCacheSql;
		return;
	}
	
	MyVariants params;
	vector<int8_t> paramTypes;
	readParams(in, params, paramTypes);
	if (task->errorCode == SQLCacheErrorCode::scecInvalidCacheSql) {
		try {	
			task->updateCount = m_connectors[thIndex]->update(sql, params, paramTypes);
		}
		catch (SQLConnectorException &) {
			task->updateCount = 0;
			task->errorCode = SQLCacheErrorCode::scecSqlFail;
			return;
		}

		flushAllTableCache(&listener, task, thIndex);
		return;
	}
	// one delete statement transform to one select statement and one delete statement
	std::string selectSQL = "SELECT * FROM ";
	selectSQL.append(listener.tableSchema()->name());
	
	std::string::size_type wherePos = StrUtils::toUpper(sql).find(" WHERE ");
	if (wherePos != std::string::npos) {
		selectSQL.append(sql.substr(wherePos));
	}

	SQLNormalTable resultTable(listener.tableSchema());
	resultTable.setThreadIndex(thIndex);
	m_connectors[thIndex]->select(selectSQL, params, paramTypes, &resultTable);
	if (resultTable.recordCount() == 0) {
		return;
	}

	std::string newDeleteSQL = sql;
	if (wherePos != std::string::npos) {
		newDeleteSQL = sql.substr(0, wherePos + 7);
		FieldSchema* pkField = static_cast<SQLNormalTableSchema*>(resultTable.schema())->primaryKey();
		newDeleteSQL.append(pkField->name());
		int nRecCount = resultTable.recordCount();
		if (nRecCount == 1) {
			resultTable.forEach([&](SQLRecord *rec) {
				newDeleteSQL.append(" = ").append(rec->strValue(pkField->name()));
				});
		}
		else {
			newDeleteSQL.append(" IN (");
			resultTable.forEach([&](SQLRecord *rec) {
				newDeleteSQL.append(rec->strValue(pkField->name()));
				if (--nRecCount > 0) {
					newDeleteSQL.append(",");
				}
				});
			newDeleteSQL.append(")");
		}
	}

	try {
		MyVariants emptyParams;
		vector<int8_t> emptyParamTypes;
		task->updateCount = m_connectors[thIndex]->update(newDeleteSQL, emptyParams, emptyParamTypes);
	}
	catch (SQLConnectorException &) {
		task->updateCount = 0;
		task->errorCode = SQLCacheErrorCode::scecSqlFail;
	}

	if (task->updateCount > 0) {
		writeUpdateTable(resultTable, UpdateOperation::umDelete, task->buffer);
	}
}
// 单线程执行
void SQLContext::doUpdate(WriteTaskData *task, int thIndex)
{
	InputStream in(task->sqlBytes);
	std::string sql = in.readText();
	ANTLRInputStream input(formatAntlrSql(sql));
	MySqlLexer lexer(&input);
	CommonTokenStream tokens(&lexer);
	MySqlParser parser(&tokens);
	MySQLUpdateExprListener listener(this);
	auto sqlStat = parser.sqlStatements();
	if (parser.getNumberOfSyntaxErrors() > 0) {
		task->errorCode = SQLCacheErrorCode::scecInvalidSql;
		return;
	}

	try {
		tree::ParseTreeWalker::DEFAULT.walk(&listener, sqlStat);
	}
	catch (...) {
		task->errorCode = SQLCacheErrorCode::scecInvalidCacheSql;
	}
	
	MyVariants params;
	vector<int8_t> paramTypes;
	readParams(in, params, paramTypes);
	if (task->errorCode == SQLCacheErrorCode::scecInvalidCacheSql) {
		try {	
			task->updateCount = m_connectors[thIndex]->update(sql, params, paramTypes);
		}
		catch (SQLConnectorException &) {
			task->updateCount = 0;
			task->errorCode = SQLCacheErrorCode::scecSqlFail;
			return;
		}
		
		flushAllTableCache(&listener, task, thIndex);
		return;
	}

	// 一条update语句转换成1条select语句和update语句
	std::string selectSQL = "SELECT *, ";
	SQLExtendTableSchema *updateTableSchema = new SQLExtendTableSchema(listener.tableSchema());
	for (int i = 0; i < listener.updateFieldCount(); ++i) {
		FieldSchema *updateField = listener.updateField(i);
		std::string newUpdateFieldName = std::string(updateField->name()).append(UPDATE_EXPR_SUFFIX);
		StrUtils::append(selectSQL, "? AS ", newUpdateFieldName);
		if (i < listener.updateFieldCount() - 1) {
			selectSQL.append(", ");
		}

		updateTableSchema->addField(newUpdateFieldName, updateField->dataType());
	}
	updateTableSchema->compile();

	StrUtils::append(selectSQL, " FROM ", listener.tableSchema()->name());

	std::string::size_type wherePos = StrUtils::toUpper(sql).find(" WHERE ");
	if (wherePos != std::string::npos) {
		StrUtils::append(selectSQL, sql.substr(wherePos));
	}

	SQLNormalTable resultTable(updateTableSchema);
	resultTable.setThreadIndex(thIndex);
	m_connectors[thIndex]->select(selectSQL, params, paramTypes, &resultTable);
	if (resultTable.recordCount() == 0) {
		return;
	}

	FieldSchema *pkField = static_cast<SQLNormalTableSchema *>(resultTable.schema())->primaryKey();
	std::string newUpdateSQL = sql;
	if (wherePos != std::string::npos) {
		newUpdateSQL = sql.substr(0, wherePos + 7);
		newUpdateSQL.append(pkField->name());
		int nRecCount = resultTable.recordCount();
		if (nRecCount == 1) {
			resultTable.forEach([&](SQLRecord *rec) {
				StrUtils::append(newUpdateSQL, " = " , rec->strValue(pkField->name()), ";");
			});
		}
		else {
			newUpdateSQL.append(" IN (");
			resultTable.forEach([&](SQLRecord *rec) {
				newUpdateSQL.append(rec->strValue(pkField->name()));
				if (--nRecCount > 0) {
					newUpdateSQL.append(",");
				}
			});
			newUpdateSQL.append(");");
		}
	}
	
	try {
		for (int i = 0; i < params.count() - listener.updateFieldCount(); ++i) {
			params.remove(params.count() - 1);
		}
		paramTypes.erase(paramTypes.begin() + listener.updateFieldCount(), paramTypes.end());
		task->updateCount = m_connectors[thIndex]->update(newUpdateSQL, params, paramTypes);
	}
	catch (SQLConnectorException &) {
		task->updateCount = 0;
		task->errorCode = SQLCacheErrorCode::scecSqlFail;
	}

	if (task->updateCount > 0) {
		exchangeUpdateFields(resultTable);
		writeUpdateTable(resultTable, UpdateOperation::umModify, task->buffer);
	}
}

void SQLContext::doTransaction(WriteTaskData *task, int thIndex)
{
	InputStream in(task->sqlBytes);
	task->buffer->writeUByte(0xFF);
	int32_t pos = task->buffer->writePos();
	int sqlCount = 0;
	while (!in.atEnd()) {
		int32_t startPos = in.pos();
		std::string commandStr = in.readText();
		CommandType type = parseCommandType(commandStr);
		if (type == CommandType::ctStartTransaction) {
			m_connectors[thIndex]->startTransaction();
			continue;
		}
		else if (type == CommandType::ctCommit) {
			m_connectors[thIndex]->commit();
			break;
		}

		WriteTaskData curTask;
		curTask.sqlBytes = ByteArray::directFrom(task->sqlBytes->data() + startPos, 
			task->sqlBytes->byteLength() - startPos);
		switch (type) {
		case CommandType::ctInsert:
			doInsert(&curTask, thIndex);
			break;
		case CommandType::ctDelete:
			doRemove(&curTask, thIndex);
			break;
		case CommandType::ctUpdate:
			doUpdate(&curTask, thIndex);
			break;
		}

		if (curTask.errorCode != SQLCacheErrorCode::scecNone) {
			m_connectors[thIndex]->rollBack();
			task->updateCount = 0;
			task->errorCode = curTask.errorCode;
			return;
		}

		task->updateCount += curTask.updateCount;
		++sqlCount;
	}

	task->buffer->seek(pos);
	task->buffer->writeUByte(sqlCount);
}

void SQLContext::doUpdateCache(UpdateCacheTaskData *task, int thIndex)
{
	prepareUpdateCacheTaskData(task, thIndex);
	updateAffectedCacheTable(task, thIndex);
	uint32_t c = task->referCount.fetch_sub(1);
	if (c == 0) {
		// free UpdateCacheTaskData
		m_taskQueues[task->threadIndex]->addNewTask(TaskType::ttFreeUpdateCacheTask, task);
	}
}

void SQLContext::doPushBlock(PushBlockTaskData *task, int thIndex)
{
	// array block
	if (task->type == 1) {
		MemoryManager::instantce(thIndex).arrayMemory().pushBlock(task);
	}
	else if (task->type == 2) {
		// var block
		MemoryManager::instantce(thIndex).varMemory().pushBlock(task);
	}
}

void SQLContext::doReset(int thIndex)
{
	delete m_graphs[thIndex];
	m_graphs[thIndex] = new SQLGraph(thIndex);
	for (auto j = m_cacheTableSchemas[thIndex].begin(); j != m_cacheTableSchemas[thIndex].end(); ++j) {
		delete j->second->schema;
		delete j->second;
	}
	m_cacheTableSchemas[thIndex].clear();
	SQLTableContainer::instance(thIndex)->reset();
	
	MemoryManager::instantce(thIndex).arrayMemory().reset();
	MemoryManager::instantce(thIndex).varMemory().reset();
}

void SQLContext::doFreeUpdateCacheTask(UpdateCacheTaskData* task)
{
	delete reinterpret_cast<vector<FieldSchema*>*>(task->updateFields);
	delete reinterpret_cast<SQLTempTable*>(task->updateRecords);
	task->rawData.reset();
	delete task;
}

void SQLContext::setTaskFinish(TaskData *task, int thIndex)
{
	std::unique_lock<std::mutex> lk(*m_waitTaskLocks[thIndex]);
	task->isFinish = true;
	m_waitTaskConds[thIndex]->notify_all();
}

int SQLContext::strHash(const std::string &sqlStr)
{
	int result = 0;
	for (int i = 0; i < sqlStr.size(); ++i) {
		result += sqlStr[i];
	}
	return result & (m_threadCnt - 1);
}

void SQLContext::flushAllTableCache(MySQLExprListener *listener, WriteTaskData *task, int thIndex)
{
	SQLNormalTable resultTable(listener->tableSchema());
	resultTable.setThreadIndex(thIndex);
	writeUpdateTable(resultTable, UpdateOperation::umAll, task->buffer);
}

void SQLContext::updateAffectedCacheTable(UpdateCacheTaskData *task, int thIndex)
{
	unordered_map<SQLSchemaVertex *, uint8_t> tableSchemas;
	auto updateFields = reinterpret_cast<vector<FieldSchema *> *>(task->updateFields);
	for (int i = 0; i < updateFields->size(); ++i) {
		findEffectedCacheTable((*updateFields)[i], m_graphs[thIndex], tableSchemas);
	}

	auto updateRecords = reinterpret_cast<SQLTempTable *>(task->updateRecords);
	SQLExtendRecord *eRec = nullptr;
	for (auto i = tableSchemas.begin(); i != tableSchemas.end(); ++i) {
		SQLSchemaVertex *schemaVtx = i->first;
		if (task->updateMode == UpdateOperation::umAll || schemaVtx->schema()->isGroupBy()) {
			schemaVtx->clearTable(thIndex);
			continue;
		}

		if (task->updateMode == UpdateOperation::umInsert) {
			insertUpdateRecords(updateRecords, schemaVtx, thIndex);
		}
		else if (task->updateMode == UpdateOperation::umDelete) {
			deleteUpdateRecords(updateRecords, schemaVtx, thIndex);
		}
		else {
			if (RelationUtils::isWhere(i->second)) {
				if (!eRec) {
					eRec = new SQLExtendRecord();
					for (int i = 0; i < updateFields->size(); ++i) {
						auto field = (*updateFields)[i];
						const string &fieldName = field->name();
						eRec->addMapFields(fieldName,
							fieldName.substr(0, fieldName.length() - UPDATE_EXPR_SUFFIX.length()));
					}
				}
				// remove old value record from table, then fill new value, add record to new Table
				deleteUpdateRecords(updateRecords, schemaVtx, thIndex, eRec);
				insertUpdateRecords(updateRecords, schemaVtx, thIndex);
			}
			else {
				updateUpdateRecords(updateRecords, schemaVtx, updateFields, thIndex);
			}
		}
	}

	if (eRec) {
		delete eRec;
	}
}

void SQLContext::findEffectedCacheTable(FieldSchema *updateField, SQLGraph *graph, 
	std::unordered_map<SQLSchemaVertex *, uint8_t> &tableSchemas)
{
	SQLVertex *vtx = graph->findVertex(reinterpret_cast<intptr_t>(updateField));
	if (!vtx) {
		return;
	}

	SQLFieldVertex *fieldVtx = static_cast<SQLFieldVertex *>(vtx);
	for (int i = 0; i < fieldVtx->edgeCount(); ++i) {
		SQLEdge *edge = fieldVtx->edge(i);
		SQLSchemaVertex *schemaVtx = static_cast<SQLSchemaVertex *>(edge->to());
		auto j = tableSchemas.find(schemaVtx);
		if (j == tableSchemas.end()) {
			tableSchemas[schemaVtx] = edge->relations();
		}
		else {
			RelationUtils::set(j->second, edge->relations());
		}
	}
}

void SQLContext::insertJoinTableRecord(SQLJoinTable *joinTable, const std::string &tableName, 
	std::shared_ptr<Condition> condition, SQLRecord *rec, MyVariants &params,
	SQLConnector &connector)
{
	std::string matchTableName = joinTable->left().name() == tableName ?
		joinTable->right().name() : joinTable->left().name();

	std::string matchSelectSql = "SELECT ";
	SQLNormalTable *matchTable = joinTable->getTable(matchTableName);
	SQLNormalTableSchema *tableSchema = matchTable->normalSchema();
	for (int i = 0; i < tableSchema->fieldCount(); ++i) {
		matchSelectSql.append(tableSchema->field(i)->name());
		if (i < tableSchema->fieldCount() - 1) {
			matchSelectSql.append(",");
		}
		else {
			matchSelectSql.append(" FROM ").append(matchTableName);
		}
	}

	MyVariants newParams;
	vector<int8_t> paramTypes;
	if (condition) {
		matchSelectSql.append(" WHERE ");
		vector<int16_t> paramIndex;
		condition->toString(matchSelectSql, rec, paramIndex);
		for (int i = 0; i < paramIndex.size(); ++i) {
			newParams.add(params.variant(paramIndex[i]));
			paramTypes.push_back(variantTypeToParamType(newParams.variant(i)));
		}
	}
	
	std::vector<SQLRecord *> matchRecs = connector.select(matchSelectSql, newParams, paramTypes,
		matchTable, true);
	if (matchRecs.size() > 0) {
		SQLRecord *realRec = joinTable->getTable(tableName)->insert(rec);
		for (int i = 0; i < matchRecs.size(); ++i) {
			SQLJoinRecord *joinRec = static_cast<SQLJoinRecord *>(joinTable->newRecord());
			joinRec->add(realRec);
			joinRec->add(matchRecs[i]);
			joinTable->append(joinRec);
		}
	}
}

void SQLContext::insertUpdateRecords(SQLTempTable *updateRecords, SQLSchemaVertex *schemaVtx, int thIndex)
{
	updateRecords->forEach([&](SQLRecord *rec) {
		vector<SQLTable *> tables = schemaVtx->findTable(rec, thIndex);
		for (int i = 0; i < tables.size(); ++i) {
			SQLTable *table = tables[i];
			if (table->kind() == TableKind::tkNormal) {
				static_cast<SQLNormalTable *>(table)->insert(rec);
			}
			else {
				insertJoinTableRecord(static_cast<SQLJoinTable *>(table), 
					updateRecords->normalSchema()->name(), schemaVtx->condition(), rec, 
					table->params(), *m_connectors[thIndex]);
			}
		}
	});
}

void SQLContext::deleteUpdateRecords(SQLTempTable *updateRecords, SQLSchemaVertex *schemaVtx, 
	int thIndex, SQLExtendRecord *eRecord)
{
	updateRecords->forEach([&](SQLRecord *rec) {
		if (eRecord) {
			eRecord->setBase(rec);
			rec = eRecord;
		}

		vector<SQLTable *> tables = schemaVtx->findTable(rec, thIndex);
		for (int i = 0; i < tables.size(); ++i) {
			SQLTable *table = tables[i];
			if (table->kind() == TableKind::tkNormal) {
				static_cast<SQLNormalTable *>(table)->remove(rec);
			}
			else {
				std::string tableName =
					static_cast<SQLNormalTableSchema *>(updateRecords->schema())->name();
				static_cast<SQLJoinTable *>(table)->remove(tableName, rec);
			}
		}
	});
}

void SQLContext::updateUpdateRecords(SQLTempTable *updateRecords, SQLSchemaVertex *schemaVtx, 
	std::vector<FieldSchema *> *updateFields, int thIndex)
{
	vector<string> updateFieldNames;
	for (int i = 0; i < updateFields->size(); ++i) {
		updateFieldNames.push_back((*updateFields)[i]->name());
	}

	updateRecords->forEach([&](SQLRecord *rec) {
		vector<SQLTable *> tables = schemaVtx->findTable(rec, thIndex);
		for (int i = 0; i < tables.size(); ++i) {
			SQLTable *table = tables[i];
			if (table->kind() == TableKind::tkNormal) {
				static_cast<SQLNormalTable *>(table)->update(rec, updateFieldNames);
			}
			else {
				std::string tableName =
					static_cast<SQLNormalTableSchema *>(updateRecords->schema())->name();
				static_cast<SQLJoinTable *>(table)->update(tableName, rec, updateFieldNames);
			}
		}
	});
}

void SQLContext::exchangeUpdateFields(SQLNormalTable &resultTable)
{
	auto tableSchema = reinterpret_cast<SQLExtendTableSchema *>(resultTable.schema());
	// 用带后缀UPDATE_EXPR_SUFFIX的字段值赋值不带后缀的，得到最新记录值
	for (int i = 0; i < tableSchema->extendFieldCount(); ++i) {
		const std::string &fieldName = tableSchema->extendField(i)->name();
		std::string oldFieldName = fieldName.substr(0, fieldName.length() - UPDATE_EXPR_SUFFIX.length());
		resultTable.forEach([&](SQLRecord *rec) {
			MyVariant oldValue = rec->value(oldFieldName);
			rec->setValue(oldFieldName, rec->value(fieldName));
			rec->setValue(fieldName, oldValue);
		});
	}
}

void SQLContext::sendUpdateData(ByteArray data, WriteTaskData *task)
{
	ByteArray extInfo = task->extInfo;
	int updateCount = task->updateCount;
	uint32_t len = data->byteLength() + extInfo->byteLength();
	ByteArray result = ByteArray::from(len + 4);
	result->setUint32(0, len);
	result->assign(data, (uint32_t)4);
	result->assign(extInfo, data->byteLength());
	result->setInt32(len, updateCount);
	//send(result);
}

bool SQLContext::send(WriteBuffer *buffer, uint32_t offset)
{
	lock_guard<mutex> locker(m_sendLock);
	if (m_sendBuff) {
		bufferevent_write(m_sendBuff, buffer->dataPtr() + offset, buffer->byteLength() - offset);
		return true;
	}

	return false;
}

void SQLContext::writeUpdateTable(SQLNormalTable &updateTable, UpdateOperation mode, 
	WriteBuffer *buffer)
{
	SQLTableSchema *tableSchema = updateTable.schema();
	buffer->writeUByte((uint8_t)mode);
	int32_t startPos = buffer->writePos();
	buffer->writeUInt(0);
	buffer->writeString(tableSchema->name());

	if (tableSchema->kind() == TableKind::tkExtend) {
		SQLExtendTableSchema *extendSchema = static_cast<SQLExtendTableSchema *>(tableSchema);
		buffer->writeUByte(extendSchema->extendFieldCount());
		for (int i = 0; i < extendSchema->extendFieldCount(); ++i) {
			FieldSchema *fd = extendSchema->extendField(i);
			buffer->writeString(fd->name());
			buffer->writeUByte((uint8_t)fd->dataType());
		}
	}
	else {
		buffer->writeUByte(0);
	}

	uint32_t recCount = updateTable.recordCount();
	buffer->writeUInt(recCount);
	updateTable.forEach([&](SQLRecord *rec){
		rec->save(buffer);
	});

	int32_t endPos = buffer->writePos();
	buffer->seek(startPos);
	buffer->writeUInt(endPos - startPos - 4);
	buffer->seek(endPos);
}

SQLTempTable *SQLContext::readUpdateTable(ByteArray data, int thIndex)
{
	InputStream in(data);
	string tableName = in.readString();
	SQLNormalTableSchema *tableSchema = findTable(tableName);
	if (!tableSchema) {
		std::cerr << "invalid table name" << std::endl;
		return nullptr;
	}

	SQLTempTable *tableObj = nullptr;
	int fieldCount = in.readUByte();
	if (fieldCount > 0) {
		SQLExtendTableSchema *extendSchema = new SQLExtendTableSchema(tableSchema);
		for (int i = 0; i < fieldCount; ++i) {
			string fieldName = in.readString();
			DataType dataType = (DataType)(in.readUByte());
			extendSchema->addField(fieldName, dataType);
		}

		extendSchema->compile();
		tableObj = new SQLTempTable(extendSchema);
	}
	else {
		tableObj = new SQLTempTable(tableSchema);
	}

	tableObj->setThreadIndex(thIndex);
	tableObj->setData(data.get());
	uint32_t recCount = in.readUInt();
	for (int i = 0; i < recCount; ++i) {
		SQLTempRecord *rec = static_cast<SQLTempRecord *>(tableObj->newRecord());
		rec->load(in);
		tableObj->append(rec);
	}

	return tableObj;
}
// choose task-thread having the least task count
int SQLContext::balanceChoose()
{
	int index = 0;
	int32_t minCount = m_taskQueues[0]->count();
	for (int i = 1; i < m_threadCnt; ++i) {
		int32_t count = m_taskQueues[i]->count();
		if (count < minCount) {
			index = i;
			minCount = count;
		}
	}

	return index;
}

/*
* the most busy thread assure these:
* all other thread's task(to do) quantity is less more than MOST_BUSY_GAP
*/
bool SQLContext::isMostBusy(int thIndex)
{
	int32_t maxCount = m_taskQueues[thIndex]->count();
	for (int i = 1; i < m_threadCnt; ++i) {
		if (i == thIndex) {
			continue;
		}

		int32_t count = m_taskQueues[i]->count();
		if (count >= maxCount - MOST_BUSY_GAP) {
			return false;
		}
	}

	return true;
}

void SQLContext::readParams(InputStream& in, MyVariants &params, 
	std::vector<int8_t>& paramTypes)
{
	int count = in.readShort();
	for (int i = 0; i < count; ++i) {
		int8_t type = in.readByte();
		paramTypes.push_back(type);

		bool isNull = in.readBoolean();
		if (isNull) {
			params.add(nullptr);
			continue;
		}

		switch (ParamDataType(type)) {
			case ParamDataType::pdtString:
				params.add(in.readText());
				break;
			case ParamDataType::pdtBool:
				params.add(in.readByte() == 1 ? true : false);
				break;
			case ParamDataType::pdtInt:
				params.add(in.readInt());
				break;
			case ParamDataType::pdtLong:
				params.add(in.readLong());
				break;
			case ParamDataType::pdtDouble:
				params.add(in.readDouble());
				break;
			case ParamDataType::pdtBlob:
				params.add(in.readBlock());
				break;
			default:
				break;
		}
	}
}

int8_t SQLContext::variantTypeToParamType(const MyVariant& param)
{
	if (param.type() == MyValueType::mvtString) {
		return (int8_t)ParamDataType::pdtString;
	}
	else if (param.type() == MyValueType::mvtBool) {
		return (int8_t)ParamDataType::pdtBool;
	}
	else if (param.type() == MyValueType::mvtInt32) {
		return (int8_t)ParamDataType::pdtInt;
	}
	else if (param.type() == MyValueType::mvtInt64) {
		return (int8_t)ParamDataType::pdtLong;
	}
	else if (param.type() == MyValueType::mvtDouble) {
		return (int8_t)ParamDataType::pdtDouble;
	}
	else if (param.type() == MyValueType::mvtBlob) {
		return (int8_t)ParamDataType::pdtBlob;
	}
}

const std::string SQLContext::formatAntlrSql(const std::string& input)
{
	return StrUtils::replace(input, "?", "'?'");
}

void SQLContext::prepareUpdateCacheTaskData(UpdateCacheTaskData* task, int thIndex)
{
	lock_guard<mutex> locker(*m_updateCacheLocks[task->lockIndex]);
	if (task->updateRecords != 0) {
		return;
	}

	if (task->rawData) {
		SQLTempTable* updateRecords = readUpdateTable(task->rawData, thIndex);
		task->updateRecords = reinterpret_cast<intptr_t>(updateRecords);

		vector<FieldSchema*>* updateFields = new vector<FieldSchema*>();
		task->updateFields = reinterpret_cast<intptr_t>(updateFields);
		task->threadIndex = thIndex;
		task->referCount = m_threadCnt;

		if (task->updateMode == UpdateOperation::umModify) {
			SQLExtendTableSchema* extendSchema = static_cast<SQLExtendTableSchema*>(updateRecords->normalSchema());
			SQLNormalTableSchema* referTable = findTable(extendSchema->name());

			for (int i = 0; i < extendSchema->extendFieldCount(); ++i) {
				string fieldName = extendSchema->extendField(i)->name();
				fieldName = fieldName.substr(0, fieldName.length() - UPDATE_EXPR_SUFFIX.length());
				FieldSchema* referField = referTable->findField(fieldName);
				if (referField) {
					updateFields->push_back(referField);
				}
				else {
					std::cerr << "find update field go wrong" << std::endl;
				}
			}
		}
		else {
			updateRecords->normalSchema()->copyFieldSchemas(*updateFields);
		}
	}
}

void SQLContext::addTaskThread(int taskIndex)
{
	std::unique_lock<std::mutex> lk(m_threadLock);
	bool ok = m_taskQueues[taskIndex]->setThreadId(m_threads.size());
	if (ok) {
		m_threads.push_back(new thread(threadFunc, this, taskIndex, m_threads.size()));
	}
}

int SQLContext::balanceChooseForSql(const std::string &sql)
{
	// sqls with same schema is put the same thread as far as possible
	int thIndex = strHash(sql);
	return isMostBusy(thIndex) ? thIndex + 1:thIndex; // linear probing, thIndex + 1 must not be the most busy
}

void SQLContext::select(ByteArray sqlBytes, const std::string &sql, WriteBuffer* buffer)
{
	SQLInfo info;
	info.sql = sql;
	int index = balanceChooseForSql(sql);
	SelectTaskData data;
	data.sqlBytes = sqlBytes;
	data.buffer = buffer;
	buffer->writeUByte(data.errorCode);
	
	{
		std::unique_lock<std::mutex> lk(*m_waitTaskLocks[index]);
		m_taskQueues[index]->addNewTask(TaskType::ttSelect, &data);
		bool timeOut = !m_waitTaskConds[index]->wait_for(lk, std::chrono::minutes(MAX_READ_TASK_TIME), [&data] {
			return data.isFinish;
			});
		if (timeOut) {
			// discard the index task thread, and create new thread
			addTaskThread(index);

			std::unique_lock<std::mutex> lk2(*m_waitTaskLocks[index]);
			m_waitTaskConds[index]->wait_for(lk2, std::chrono::minutes(MAX_READ_TASK_TIME), [&data] {
				return data.isFinish;
				});
		}
	}
	buffer->seek(0);
	buffer->writeUByte(data.errorCode);
}

void SQLContext::execUpdate(ByteArray sqlBytes, TaskType type, 
	ByteArray extInfo, WriteBuffer* buffer)
{
	int index = balanceChoose();
	WriteTaskData data;
	data.sqlBytes = sqlBytes;
	if (!m_readMode) {
		data.extInfo = extInfo;  // in readMode, extInfo is Empty
	}
	data.buffer = buffer;
	buffer->writeUInt(0);
	buffer->writeUByte(data.errorCode);
	buffer->writeInt(data.updateCount);
	if (!m_readMode) {
		buffer->writeBytes(extInfo);
	}

	{
		std::unique_lock<std::mutex> lk(*m_waitTaskLocks[index]);
		m_taskQueues[index]->addNewTask(type, &data);
		bool timeOut = !m_waitTaskConds[index]->wait_for(lk, std::chrono::minutes(MAX_WRITE_TASK_TIME), [&data] {
			return data.isFinish;
			});

		if (timeOut) {
			// discard the index task thread, and create new thread
			if (!m_readMode) {
				addTaskThread(index);
			}

			std::unique_lock<std::mutex> lk2(*m_waitTaskLocks[index]);
			m_waitTaskConds[index]->wait_for(lk2, std::chrono::minutes(MAX_WRITE_TASK_TIME), [&data] {
				return data.isFinish;
				});
		}
	}

	buffer->seek(0);
	if (m_readMode) {
		buffer->writeUInt(5);
		buffer->writeUByte(data.errorCode);
		buffer->writeInt(data.updateCount);
	}
	else {
		buffer->writeUInt(buffer->byteLength() - 4);
		buffer->writeUByte(data.errorCode);
		buffer->writeInt(data.updateCount);
		//send(buffer);
	}	
}

void SQLContext::syncWrite(ByteArray data)
{
	// Packet Sticky problem exist
	if (!m_buffer) {
		InputStream in(data);
		uint32_t len = in.readUInt();
		data = data->slice(4);
		m_buffer = ByteArray::from(len);
		m_bufferLen = 0;
	}

	uint32_t toRead = m_buffer->byteLength() - m_bufferLen;
	if (data->byteLength() <= toRead) {
		m_buffer->assign(data, m_bufferLen);
		m_bufferLen += data->byteLength();
		data.reset();
	}
	else {
		m_buffer->assign(data->slice(0, toRead), m_bufferLen);
		m_bufferLen = m_buffer->byteLength();
		data = data->slice(toRead);
	}

	if (m_bufferLen == m_buffer->byteLength()) {
		// errorCode(1byte)|updateCount(4byte)|threadIndex(1byte)|taskDataPtr(8byte)|updateData(all left bytes)
		auto data = reinterpret_cast<WriteTaskData *>(m_buffer->getUint64(6));
		data->errorCode = m_buffer->getUint8(0);
		data->updateCount = m_buffer->getInt32(1);
		int thIndex = m_buffer->getUint8(5);
		if (data->updateCount > 0) {
			addUpdateCacheTask(m_buffer->slice(14));
		}

		m_buffer.reset();
		setTaskFinish(data, thIndex);
	}

	if (data) {
		syncWrite(data);
	}
}

void SQLContext::addUpdateCacheTask(ByteArray input)
{
	m_lockIndex = (m_lockIndex + 1) % m_threadCnt;
	if (input->getUint8(0) == 0xFF) {
		// is transaction, other update data ,this byte is mode, less than 10
		uint8_t count = input->getUint8(1);
		uint32_t pos = 2;
		vector<TaskType> types;
		vector<TaskData*> datas;
		for (int i = 0; i < count; ++i) {
			UpdateCacheTaskData* task = new UpdateCacheTaskData;
			task->updateMode = (UpdateOperation)(input->getUint8(pos));
			uint32_t size = input->getUint32(pos + 1);
			task->rawData = input->slice(pos + 5, pos + 5 + size);
			task->referCount = m_threadCnt;
			task->lockIndex = m_lockIndex;
			types.push_back(TaskType::ttUpdateCache);
			datas.push_back(task);
			pos += size + 5;
		}

		for (int i = 0; i < m_threadCnt; ++i) {
			m_taskQueues[i]->batchAddNewTask(types, datas);
		}
	}
	else {
		UpdateCacheTaskData* task = new UpdateCacheTaskData;
		task->updateMode = (UpdateOperation)(input->getUint8(0));
		task->rawData = input->slice(5);
		task->referCount = m_threadCnt;
		task->lockIndex = m_lockIndex;
		for (int i = 0; i < m_threadCnt; ++i) {
			m_taskQueues[i]->addNewTask(TaskType::ttUpdateCache, task);
		}
	}
}

void SQLContext::reset()
{
	if (!m_readMode) {
		return;
	}

	for (int i = 0; i < m_threadCnt; ++i) {
		m_taskQueues[i]->addNewTask(TaskType::ttReset, NULL);
	}
}

bool SQLContext::connect(const std::string &serverAddr)
{
	std::vector<std::string> connectStrs = StrUtils::split(serverAddr, ',');
	if (connectStrs.size() != 4) {
		return false;
	}

	for (int i = 0; i < m_connectors.size(); ++i) {
		if (!m_connectors[i]->connect(connectStrs[0], connectStrs[1], connectStrs[2], connectStrs[3])) {
			std::cerr << "connect sql server fail" << std::endl;
			return false;
		}
	}

	vector<SQLNormalTableSchema*> tableSchemas;
	m_connectors[0]->buildAllTableSchemas(tableSchemas);
	for (int i = 0; i < tableSchemas.size(); ++i) {
		auto tableSchema = tableSchemas[i];
		m_tableSchemas[tableSchema->name()] = tableSchema;
		tableSchema->compile();
	}
	return true;
}

Task *SQLContext::fetchTask(int index, int threadId)
{
	return m_taskQueues.at(index)->fetchTask(threadId);
}

void SQLContext::executeTask(Task *task, int thIndex)
{
	switch (task->type)
	{
	case TaskType::ttInsert:
	case TaskType::ttUpdate:
	case TaskType::ttDelete:
	case TaskType::ttTransaction:
	{
		doWrite(task, thIndex);
		break;
	}
	case TaskType::ttSelect:
	{
		auto data = reinterpret_cast<SelectTaskData *>(task->data);
		doSelect(data, thIndex);
		setTaskFinish(data, thIndex);
		break;
	}
	case TaskType::ttUpdateCache:
	{
		doUpdateCache(reinterpret_cast<UpdateCacheTaskData *>(task->data), thIndex);
		break;
	}
	case TaskType::ttPushBlock:
	{
		doPushBlock(reinterpret_cast<PushBlockTaskData *>(task->data), thIndex);
		break;
	}
	case TaskType::ttReset:
	{
		doReset(thIndex);
		break;
	}
	case TaskType::ttFreeUpdateCacheTask:
	{
		doFreeUpdateCacheTask(reinterpret_cast<UpdateCacheTaskData*>(task->data));
	}
	default:
		break;
	}
}

bufferevent *SQLContext::sendBuff() const
{
	return m_sendBuff;
}

void SQLContext::setSendBuff(bufferevent *client)
{
	lock_guard<mutex> locker(m_sendLock);
	m_sendBuff = client;
}

void SQLContext::tryResetSendBuff(bufferevent *client)
{
	lock_guard<mutex> locker(m_sendLock);
	if (m_sendBuff == client) {
		m_sendBuff = nullptr;
	}
}

uint32_t SQLContext::extInfoLength() const
{
	return EXT_INFO_LEN;
}
