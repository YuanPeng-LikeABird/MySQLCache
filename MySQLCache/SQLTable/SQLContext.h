#pragma once

#include <unordered_map>
#include <string>
#include <thread>
#include <memory>
#include <unordered_set>
#include <mutex>
#include <condition_variable>
#include "MyVariant.h"
#include "ByteArray.h"
#include "SQLConnectorFactory.h"
#include "SQLGraph.h"
#include "Task.h"

class SQLTableSchema;
class SQLNormalTableSchema;
class SQLTable;
class SQLNormalTable;
class SQLJoinTable;
class SQLTempTable;
class TaskQueue;
class SQLExtendRecord;
class MySQLSelectExprListener;
class MySQLExprListener;
struct bufferevent;

class SQLContext
{
public:
	struct SQLInfo
	{
		std::string schema;
		MyVariants params;
		std::string sql;
	};

	struct SQLTableSchemaInfo
	{
		std::string addSql;
		SQLTableSchema *schema;
	};

	typedef std::unordered_map <std::string, SQLTableSchemaInfo * > TableSchemaHash;
	typedef std::unordered_map <std::string, SQLNormalTableSchema * > NormalTableSchemaHash;
	typedef std::unordered_map <std::string, uint32_t > TableHash;

public:
	SQLContext(bool readMode, int threadCount, const std::string &serverAddr, 
		const std::string &sqlType = "mysql", 
		bool enableMonitor = false);
	~SQLContext();

	static SQLContext *instance();
	static void setInstance(SQLContext *ctx);

	bool readMode() const;

	void free();
	void test();
	// 这个Table对应db中表结构信息
	SQLNormalTableSchema *findTable(const std::string &name);
	SQLNormalTableSchema *addTable(const std::string &name);
	// 这个Table是缓存结果表
	SQLTable *addCacheTable(SQLTableSchema *schema, int thIndex, uint32_t &tableID);
	SQLTable *selectCacheTable(const std::string &sql, MyVariants &params,
		std::vector<int8_t>& paramTypes, int thIndex);
	void directQuery(const std::string &sql, MyVariants &params,
		std::vector<int8_t>& paramTypes, int thIndex, WriteBuffer* buffer);

	SQLTableSchemaInfo *createCacheTableSchema(const std::string &sql, int thIndex);

	void select(ByteArray sqlBytes, const std::string &sql, WriteBuffer *buffer);
	void execUpdate(ByteArray sqlBytes, TaskType type, ByteArray extInfo, WriteBuffer *buffer);

	void syncWrite(ByteArray data);
	void addUpdateCacheTask(ByteArray input);
	void reset();
	bool connect(const std::string &serverAddr);

	Task *fetchTask(int index, int threadId);
	void executeTask(Task *task, int thIndex);

	struct bufferevent *sendBuff() const;

	void setSendBuff(struct bufferevent *client);
	void tryResetSendBuff(struct bufferevent *client);
	uint32_t extInfoLength() const;

private:
	void initialize(const std::string& serverAddr);

	SQLTableSchema *doCreateCacheTableSchema(const std::string &sqlStr, std::string &addSql, 
		int thIndex);

	SQLTableSchema *createGroupByTableSchema(const std::string &sqlStr, MySQLSelectExprListener *listener, 
		int thIndex);
	SQLTableSchema *createNormalTableSchema(const std::string &sqlStr, std::string &addSql, 
		MySQLSelectExprListener *listener, int thIndex);
	SQLTableSchema *createJoinTableSchema(const std::string &sqlStr, std::string &addSql, 
		MySQLSelectExprListener *listener, int thIndex);

	void addFieldVtx(SQLGraph *graph, FieldSchema *field, SQLVertex *schemaVtx, 
		bool isQuery = true, bool isWhere = false, bool isOrder = false);

	void doSelect(SelectTaskData *task, int thIndex);
	void doWrite(Task *task, int thIndex);
	void doInsert(WriteTaskData *task, int thIndex);
	void doRemove(WriteTaskData *task, int thIndex);
	void doUpdate(WriteTaskData *task, int thIndex);
	void doTransaction(WriteTaskData *task, int thIndex);
	void doUpdateCache(UpdateCacheTaskData *task, int thIndex);
	void doPushBlock(PushBlockTaskData *task, int thIndex);
	void doReset(int thIndex);
	void doFreeUpdateCacheTask(UpdateCacheTaskData* task);

	void setTaskFinish(TaskData *task, int thIndex);

	int strHash(const std::string &sqlStr);
	void flushAllTableCache(MySQLExprListener *listener, WriteTaskData *task, int thIndex);

	void updateAffectedCacheTable(UpdateCacheTaskData *task, int thIndex);

	void findEffectedCacheTable(FieldSchema *updateField, SQLGraph *graph, 
		std::unordered_map<SQLSchemaVertex *, uint8_t> &tableSchemas);

	void insertJoinTableRecord(SQLJoinTable *joinTable, const std::string &tableName, 
		std::shared_ptr<Condition> condition, SQLRecord *rec, MyVariants &params,
		SQLConnector &connector);

	void insertUpdateRecords(SQLTempTable *updateRecords, SQLSchemaVertex *schemaVtx, int thIndex);
	void deleteUpdateRecords(SQLTempTable *updateRecords, SQLSchemaVertex *schemaVtx, int thIndex,
		SQLExtendRecord *eRecord = nullptr);
	void updateUpdateRecords(SQLTempTable *updateRecords, SQLSchemaVertex *schemaVtx,
		std::vector<FieldSchema *> *updateFields, int thIndex);

	void exchangeUpdateFields(SQLNormalTable &resultTable);

	void sendUpdateData(ByteArray data, WriteTaskData *task);
	bool send(WriteBuffer *buffer, uint32_t offset = 0);

	void writeUpdateTable(SQLNormalTable &updateTable, UpdateOperation mode, WriteBuffer* buffer);
	SQLTempTable *readUpdateTable(ByteArray data, int thIndex);

	int balanceChooseForSql(const std::string& sql);
	void parseSQLInfo(SQLInfo &sqlStr);
	bool doConstToken(MyVariants &params, const std::string &token);
	bool isChinese(unsigned char c);

	int balanceChoose();
	bool isMostBusy(int thIndex);

	void readParams(InputStream& in, MyVariants &params, std::vector<int8_t>& paramTypes);
	int8_t variantTypeToParamType(const MyVariant &param);

	const std::string formatAntlrSql(const std::string& input);
	void prepareUpdateCacheTaskData(UpdateCacheTaskData* task, int thIndex);
	void addTaskThread(int taskIndex);

private:
	uint8_t m_threadCnt;
	std::vector<std::thread *> m_threads;
	std::vector<TaskQueue *> m_taskQueues;
	std::vector<SQLGraph *> m_graphs;
	std::vector<std::mutex *> m_waitTaskLocks;
	std::vector<std::condition_variable *> m_waitTaskConds;
	std::vector<std::mutex*> m_updateCacheLocks;

	NormalTableSchemaHash m_tableSchemas;
	TableSchemaHash *m_cacheTableSchemas;

	std::vector<SQLConnector *> m_connectors;

	bool m_readMode;
	bool m_enableMonitor;
	std::string m_sqlType;

	struct bufferevent *m_sendBuff;
	std::mutex m_sendLock;
	std::mutex m_threadLock;

	ByteArray m_buffer;
	uint32_t m_bufferLen;
	uint8_t m_lockIndex;
};
