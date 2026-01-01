#pragma once

#include "ByteArray.h"
#include "SQLContext.h"
#include "CacheSetting.h"
#include <string>

struct bufferevent;
class WriteBuffer;

class CacheServer
{
public:
	CacheServer();
	~CacheServer();

	void startUp(CacheSetting *setting, bool readMode);
	void shutDown();

	void test();

	void processCommand(ByteArray rawData, struct bufferevent *client, WriteBuffer *buffer);

	void setSendBuff(struct bufferevent *client);
	void tryResetSendBuff(struct bufferevent *client);
private:
	void doProcessCommand(ByteArray commandBytes, WriteBuffer *buffer,
		struct bufferevent *client = nullptr, ByteArray extInfo = ByteArray());

	void outputMonitorInfo(WriteBuffer* buffer);

private:
	SQLContext *m_context;
};