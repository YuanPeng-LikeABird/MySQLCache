#include "CacheServer.h"
#include "InputStream.h"
#include "DataType.h"
#include "SQLTableSchema.h"
#include "StrUtils.h"
#include "MemoryManager.h"
#include "CacheMonitor.h"
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <vector>

using namespace std;

CacheServer::CacheServer() :
	m_context(nullptr)
{
}

CacheServer::~CacheServer()
{
	if (m_context) {
		delete m_context;
	}
}

void CacheServer::startUp(CacheSetting *setting, bool readMode)
{
	initMemoryManagers(setting);
	m_context = new SQLContext(readMode, setting->read(WORKER_THREAD_COUNT).toInt(), 
		setting->read(SQL_SERVER_ADDR).toString());
	SQLContext::setInstance(m_context);
}

void CacheServer::shutDown()
{
	m_context->free();
}

void CacheServer::test()
{
	m_context->test();
}

void CacheServer::processCommand(ByteArray rawData, struct bufferevent *client, WriteBuffer* buffer)
{
	// is from write-server-node
	if (m_context->readMode()) {
		if (client == m_context->sendBuff()) {
			m_context->syncWrite(rawData);
			return;
		}

		// command is only one kind of command(transaction is a whole command)
		doProcessCommand(rawData, buffer, client);
	}
	else {
		uint32_t extInfoLen = m_context->extInfoLength();
		// current node is write, the command is sended by main-read-server
		ByteArray extInfo = ByteArray::directFrom(rawData->data(), extInfoLen);
		doProcessCommand(ByteArray::directFrom(rawData->data() + extInfoLen, 
			rawData->byteLength() - extInfoLen), buffer, client, extInfo);
	}
}

void CacheServer::setSendBuff(bufferevent *client)
{
	m_context->setSendBuff(client);
}

void CacheServer::tryResetSendBuff(bufferevent *client)
{
	m_context->tryResetSendBuff(client);
}

void CacheServer::doProcessCommand(ByteArray commandBytes, WriteBuffer *buffer,
	bufferevent *client, ByteArray extInfo)
{
	buffer->reset();
	InputStream in(commandBytes);
	std::string sql = in.readText();
	CommandType type = parseCommandType(sql);
	switch (type)
	{
		// reply starts with errorCode:
	case CommandType::ctSelect:
	{
		m_context->select(commandBytes, sql, buffer);
		bufferevent_write(client, buffer->dataPtr(), buffer->byteLength());
		break;
	}
	case CommandType::ctStartTransaction:
	{
		m_context->execUpdate(commandBytes, TaskType::ttTransaction, extInfo, buffer);
		bufferevent_write(client, buffer->dataPtr(), buffer->byteLength());
		break;
	}
	case CommandType::ctInsert:
		m_context->execUpdate(commandBytes, TaskType::ttInsert, extInfo, buffer);
		bufferevent_write(client, buffer->dataPtr(), buffer->byteLength());
		break;
	case CommandType::ctDelete:
		m_context->execUpdate(commandBytes, TaskType::ttDelete, extInfo, buffer);
		bufferevent_write(client, buffer->dataPtr(), buffer->byteLength());
		break;
	case CommandType::ctUpdate:
		m_context->execUpdate(commandBytes, TaskType::ttUpdate, extInfo, buffer);
		bufferevent_write(client, buffer->dataPtr(), buffer->byteLength());
		break;
	case CommandType::ctMonitor:
	{
		outputMonitorInfo(buffer);
		bufferevent_write(client, buffer->dataPtr(), buffer->byteLength());
		break;
	}
	case CommandType::ctConfirmWriteNode:
	{
		setSendBuff(client);
		break;
	}
	case CommandType::ctReset:
	{
		m_context->reset();
		break;
	}
	case CommandType::ctConnectSqlServer:
	{
		m_context->connect(sql.substr(7));
		break;
	}
	default:
		break;
	}		
	buffer->reset();
}

void CacheServer::outputMonitorInfo(WriteBuffer *buffer)
{
	std::string info = CacheMonitor::instance()->outputHitInfo();
	if (info.empty()) {
		info = " ";
	}

	buffer->writeCharacters(info, info.length());
}
