#include <iostream>
#include "antlr4-runtime.h"
#include "MySqlParser.h"
#include "MySqlLexer.h"
#include "MySQLExprListener.h"
#include "SQLContext.h"
#include "SQLTable.h"
#include "MemoryTest.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <sys/stat.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <arpa/inet.h>
#endif
#include <event2/listener.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/thread.h>
#include "CacheServer.h"
#include "CacheSetting.h"
#include "StrUtils.h"
#include "MemoryManager.h"

CacheServer *m_server = new CacheServer();

using namespace antlr4;

#define PORT 9000
#define READ_CACHE_SIZE 1024000

int g_threadIndex = 0;

struct SERVER_THREAD_CONTEXT {
	evutil_socket_t fdRead;
	evutil_socket_t fdWrite;
	struct event_base *base;
} *g_ServerThreadContexts;

static void on_read_cb(struct bufferevent *bev, void *buffer)
{
	uint8_t* dataPtr = ((WriteBuffer*)buffer)->dataPtr();
	size_t readLen = bufferevent_read(bev, dataPtr, READ_CACHE_SIZE);
	m_server->processCommand(ByteArray::from(dataPtr, readLen), bev, (WriteBuffer*)buffer);
}

static void on_read_cb_client(struct bufferevent *bev, void * buffer)
{
	uint8_t* dataPtr = ((WriteBuffer*)buffer)->dataPtr();
	size_t readLen = bufferevent_read(bev, dataPtr, READ_CACHE_SIZE);
	m_server->processCommand(ByteArray::from(dataPtr, readLen), bev, (WriteBuffer*)buffer);
}

static void on_event_cb(struct bufferevent *bev, short events, void *buffer)
{
	if (events & BEV_EVENT_EOF) {
		std::cout << "Disconnect from Server" << std::endl;
		m_server->tryResetSendBuff(bev);
		bufferevent_free(bev);
	}
	else if (events & BEV_EVENT_ERROR) {
		std::cout << "BufferEvent Error" << std::endl;
		m_server->tryResetSendBuff(bev);
		bufferevent_free(bev);
	}
}

static void on_event_cb_client(struct bufferevent *bev, short events, void *buffer)
{
	if (events & BEV_EVENT_EOF) {
		std::cout << "Disconnect from Server" << std::endl;
		bufferevent_free(bev);
	}
	else if (events & BEV_EVENT_ERROR) {
		std::cout << "BufferEvent Error" << std::endl;
		bufferevent_free(bev);
	}
	else if (events & BEV_EVENT_CONNECTED) {
		std::cout << "Connect Server" << std::endl;
		std::string iAmWriteStr = "I_AM_WRITE_NODE";
		((WriteBuffer*)buffer)->writeText(iAmWriteStr);
		m_server->setSendBuff(bev);
		bufferevent_write(bev, ((WriteBuffer*)buffer)->dataPtr(), ((WriteBuffer*)buffer)->byteLength());
	}
}

static void addClient(evutil_socket_t fd, short evt, void *arg) {
	int thIndex = (int64_t)arg;
	evutil_socket_t objFd;
	if (recv(g_ServerThreadContexts[thIndex].fdRead, (char *)&objFd, sizeof(objFd), 0) > 0) {
		struct event_base *base = g_ServerThreadContexts[thIndex].base;
		struct bufferevent *bev = bufferevent_socket_new(base, objFd, BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);

		bufferevent_enable(bev, EV_READ | EV_WRITE);
		bufferevent_setcb(bev, on_read_cb, NULL, on_event_cb, &MemoryManager::instantce(thIndex).buffer());
	}
}

static void serverThreadLoop(int thIndex) {

#ifdef _WIN32
	evthread_use_windows_threads();
#else
	evthread_use_pthreads();
#endif
	struct event_base *base = event_base_new();
	g_ServerThreadContexts[thIndex].base = base;

	struct event *ev = event_new(base, g_ServerThreadContexts[thIndex].fdRead, EV_READ | EV_PERSIST, addClient, (void *)thIndex);
	event_add(ev, nullptr);
	event_base_dispatch(base);
	event_free(ev);
	event_base_free(base);

	evutil_closesocket(g_ServerThreadContexts[thIndex].fdRead);
	evutil_closesocket(g_ServerThreadContexts[thIndex].fdWrite);
}

bool initServerThread(int threadCount) {
	g_ServerThreadContexts = new SERVER_THREAD_CONTEXT[threadCount];
	for (int i = 0; i < threadCount; ++i) {
		evutil_socket_t fds[2];
#ifdef _WIN32
		if (evutil_socketpair(AF_INET, SOCK_STREAM, 0, fds) != 0) {
#else	
		if (evutil_socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
#endif // __WIN32
			std::cout << "create pipe fail:" 
				<< evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()) << std::endl;
			return false;
		}

		evutil_make_socket_nonblocking(fds[0]);
		evutil_make_socket_nonblocking(fds[1]);

		g_ServerThreadContexts[i].fdRead = fds[0];
		g_ServerThreadContexts[i].fdWrite = fds[1];
	}

	for (int i = 0; i < threadCount; ++i) {
		std::thread th(serverThreadLoop, i);
		th.detach();
	}

	return true;
}

void freeServerThread() {
	delete[] g_ServerThreadContexts;
}

void on_accept_cb(struct evconnlistener *listener,
	evutil_socket_t fd,
	struct sockaddr *addr,
	int socklen,
	void *ctx)
{
	// write_node must be in alone event_base，because all other event_base may be block for query result
	int thIndex = (g_threadIndex++)%* reinterpret_cast<int*>(ctx);
	if (thIndex == 0 && g_threadIndex > 1) {
		thIndex = 1;
	}
	send(g_ServerThreadContexts[thIndex].fdWrite, (const char *)&fd, sizeof(fd), 0);
}

int main(int argc, char *argv[])
{
	/*MemoryTest::testOverflow();
	MemoryTest::test();
	MemoryTest::testVar();
	MemoryTest::testArray();
	return 0;*/
	bool readMode = true;
	if (argc > 1 && StrUtils::startsWith(argv[1], "-readMode")) {
		readMode = strcmp(argv[1] + 10, "true") == 0;
	}
	CacheSetting setting(exePath() + "/Cache.ini");
	m_server->startUp(&setting, readMode);
	/*m_server->test();
	m_server->shutDown();
	return 0;*/
#ifdef _WIN32
	WORD wVersionRequested;
	WSADATA wsaData;
	wVersionRequested = MAKEWORD(2, 2);
	WSAStartup(wVersionRequested, &wsaData);
#endif

	if (readMode) {
		int serverThreadCount = setting.read(SERVER_THREAD_COUNT).toInt();
		// this api is assure where do bufferevent_write,bufferevent is at once flush data
#ifdef _WIN32
		evthread_use_windows_threads();
#else
		evthread_use_pthreads();
#endif
		struct event_base *base = event_base_new();

		struct sockaddr_in serveraddr;
		serveraddr.sin_family = AF_INET;
		serveraddr.sin_port = htons(PORT);
		serveraddr.sin_addr.s_addr = INADDR_ANY;

		struct evconnlistener *listener = evconnlistener_new_bind(base,
			on_accept_cb,
			&serverThreadCount,
			LEV_OPT_REUSEABLE,
			10,
			(struct sockaddr *)&serveraddr,
			sizeof(serveraddr));

		initServerThread(serverThreadCount);
		event_base_dispatch(base);

		evconnlistener_free(listener);
		event_base_free(base);
		freeServerThread();
	}
	else {
		// write server-node
		// is a client to read server-node
#ifdef _WIN32
		evthread_use_windows_threads();
#else
		evthread_use_pthreads();
#endif
		struct event_base *base = event_base_new();
		struct sockaddr_in serveraddr;
		serveraddr.sin_family = AF_INET;
		serveraddr.sin_port = htons(PORT);
		std::string addr = setting.read(SERVER_ADDR).toString();
		inet_pton(AF_INET, addr.c_str(), &serveraddr.sin_addr);

		struct bufferevent *bev = bufferevent_socket_new(base, -1, BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);
		bufferevent_enable(bev, EV_READ | EV_WRITE);
		bufferevent_setcb(bev, on_read_cb_client, NULL, on_event_cb_client, &MemoryManager::instantce(0).buffer());

		int isConnect = 
			bufferevent_socket_connect(bev, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) == 0;
		if (!isConnect) {
			std::cerr << "connect to server error" << std::endl;
		}

		event_base_dispatch(base);
		event_base_free(base);
	}

	m_server->shutDown();
	return 0;
}