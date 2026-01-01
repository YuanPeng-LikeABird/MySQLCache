#include "MemoryManager.h"
#include "Common.h"
#include <memory>
#ifndef _WIN32
#include <cstring>
#endif

#define MAX_REGULAR_POWER_NUMBER 16
#define MAX_REGULAR_SIZE 65536
#define MEMORY_LIST_SIZE 15
#define MIN_NODE_SIZE 256

std::string g_rootPath;

uint32_t g_arrayMemoryLimit = 1 << 30;
uint32_t g_arrayMemoryPushLimit = 1000000000;
uint32_t g_varMemoryLimit = 1 << 30;
uint32_t g_varMemoryPushLimit = 1000000000;
uint32_t g_tableMemoryLimit = 1 << 30;
uint32_t g_tableMemoryPushLimit = 1000000000;
uint32_t g_writeBufferDefaultMemory = 1 << 20;

MemoryManager *g_memoryManagers = nullptr;

class MemoryGarbager
{
public:
	MemoryGarbager() {

	}

	~MemoryGarbager() {
		if (g_memoryManagers) {
			delete[] g_memoryManagers;
		}
	}
};

MemoryGarbager g_memoryGarbage;

MemoryManager::MemoryManager() :
	m_varMemory(this),
	m_arrayMemory(this),
	m_buffer(this),
	m_index(-1),
	m_taskQueue(nullptr)
{
}

MemoryManager::~MemoryManager()
{
	for (int i = 0; i < MEMORY_LIST_SIZE - 1; ++i) {
		MemoryList &mm = m_mms[i];
		PMemoryBlockHeader curBlock = mm.firstBlock;
		while (curBlock) {
			PMemoryBlockHeader nextBlock = curBlock->next;
			free(curBlock);
			curBlock = nextBlock;
		}
	}

	MemoryList &mm = m_mms[MEMORY_LIST_SIZE - 1];
	PMemoryNodeHeader curNode = mm.first;
	while (curNode) {
		PMemoryNodeHeader nextNode = curNode->next;
		free(curNode);
		curNode = nextNode;
	}

	free(m_mms);
}

MemoryManager &MemoryManager::instantce(int index)
{
	return g_memoryManagers[index];
}

std::string MemoryManager::rootPath()
{
	return g_rootPath;
}

uint32_t MemoryManager::arrayMemoryLimit()
{
	return g_arrayMemoryLimit;
}

uint32_t MemoryManager::arrayMemoryPushLimit()
{
	return g_arrayMemoryPushLimit;
}

uint32_t MemoryManager::varMemoryLimit()
{
	return g_varMemoryLimit;
}

uint32_t MemoryManager::varMemoryPushLimit()
{
	return g_varMemoryPushLimit;
}

uint32_t MemoryManager::tableMemoryLimit()
{
	return g_tableMemoryLimit;
}

uint32_t MemoryManager::tableMemoryPushLimit()
{
	return g_tableMemoryPushLimit;
}

uint32_t MemoryManager::writeBufferDefaultMemory()
{
	return g_writeBufferDefaultMemory;
}

uint32_t MemoryManager::regularMemoryLimit() const
{
	return MAX_REGULAR_SIZE - sizeof(MemoryNodeHeader);
}

uint8_t *MemoryManager::allocate(uint32_t size)
{
	PMemoryNodeHeader node;
	if (size > regularMemoryLimit()) {
		node = (PMemoryNodeHeader)malloc(sizeof(MemoryNodeHeader) + size);
		node->size = MAX_REGULAR_POWER_NUMBER + 1;
		node->prev = nullptr;
		node->next = nullptr;
		MemoryList &objMM = m_mms[MEMORY_LIST_SIZE - 1];
		listInsertLast(objMM, node);
		++objMM.total;
	}
	else {
		int32_t powerValue = 0;
		int32_t curValue = 1;
		while (curValue < size + sizeof(MemoryNodeHeader)) {
			++powerValue;
			curValue <<= 1;
		}

		if (powerValue <= 3) {
			powerValue = 3;
		}

		MemoryList &objMM = m_mms[powerValue - 3];
		if (objMM.idle == 0) {
			prepare(powerValue - 3);
		}

		node = (PMemoryNodeHeader)objMM.first;
		listRemove(objMM, node);
		listInsertLast(objMM, node);
		--objMM.idle;
	}

	return ((uint8_t *)node) + sizeof(MemoryNodeHeader);
}

void MemoryManager::recycle(uint8_t *mem)
{
	uint8_t powerSize = *(mem - 1);
	PMemoryNodeHeader node = PMemoryNodeHeader(mem - sizeof(MemoryNodeHeader));
	if (powerSize > MAX_REGULAR_POWER_NUMBER) {
		freeNode(m_mms[MEMORY_LIST_SIZE - 1], node);
	}
	else {
		recycleNode(m_mms[powerSize - 3], node);
	}
}

VarMemoryManager &MemoryManager::varMemory()
{
	return m_varMemory;
}

ArrayMemoryManager &MemoryManager::arrayMemory()
{
	return m_arrayMemory;
}

WriteBuffer& MemoryManager::buffer()
{
	return m_buffer;
}

uint8_t MemoryManager::index() const
{
	return m_index;
}

TaskQueue *MemoryManager::taskQueue() const
{
	return m_taskQueue;
}

void MemoryManager::setTaskQueue(TaskQueue *queue)
{
	m_taskQueue = queue;
}

void MemoryManager::init(int index)
{
	m_index = index;

	int size = MEMORY_LIST_SIZE * sizeof(MemoryList);
	m_mms = (PMemoryList)malloc(size);
	memset(m_mms, 0, size);
	// Ô¤·ÖÅä
	//for (int i = 0; i < MEMORY_LIST_SIZE - 1; ++i) {
	//	prepare(i);
	//}

	m_varMemory.initialize();
	m_arrayMemory.initialize();
	m_buffer.initialize();
}

void MemoryManager::freeNode(MemoryList &mm, PMemoryNodeHeader node)
{
	listRemove(mm, node);
	free((void *)node);
	--mm.total;
}

void MemoryManager::recycleNode(MemoryList &mm, PMemoryNodeHeader node)
{
	listRemove(mm, node);
	listInsertFirst(mm, node);
	++mm.idle;
}

uint8_t *MemoryManager::prepare(int scaleIndex)
{
	int32_t size = nodeSize(scaleIndex);
	uint8_t *blockPtr = (uint8_t *)malloc(sizeof(MemoryBlockHeader) + MIN_NODE_SIZE * size);
	uint8_t *nodePtr = blockPtr + sizeof(MemoryBlockHeader);
	PMemoryNodeHeader prev = nullptr;
	for (int j = 0; j < MIN_NODE_SIZE; ++j) {
		PMemoryNodeHeader(nodePtr)->prev = prev;
		PMemoryNodeHeader(nodePtr)->next = nullptr;
		PMemoryNodeHeader(nodePtr)->size = 3 + scaleIndex;
		if (prev) {
			prev->next = (PMemoryNodeHeader)nodePtr;
		}

		prev = PMemoryNodeHeader(nodePtr);
		nodePtr += size;
	}

	m_mms[scaleIndex].idle += MIN_NODE_SIZE;
	m_mms[scaleIndex].total += MIN_NODE_SIZE;
	if (m_mms[scaleIndex].first) {
		m_mms[scaleIndex].first->prev = PMemoryNodeHeader(nodePtr - size);
	}
	m_mms[scaleIndex].first = PMemoryNodeHeader(blockPtr + sizeof(MemoryBlockHeader));

	if (!m_mms[scaleIndex].firstBlock) {
		m_mms[scaleIndex].firstBlock = (PMemoryBlockHeader)blockPtr;
		m_mms[scaleIndex].lastBlock = m_mms[scaleIndex].firstBlock;		
	}
	else {
		m_mms[scaleIndex].lastBlock->next = (PMemoryBlockHeader)blockPtr;
		m_mms[scaleIndex].lastBlock = (PMemoryBlockHeader)blockPtr;
	}
	m_mms[scaleIndex].lastBlock->next = nullptr;

	if (!m_mms[scaleIndex].last) {
		m_mms[scaleIndex].last = PMemoryNodeHeader(blockPtr + sizeof(MemoryBlockHeader) 
			+(MIN_NODE_SIZE - 1) * nodeSize(scaleIndex));
	}

	return blockPtr + sizeof(MemoryBlockHeader);
}

int32_t MemoryManager::nodeSize(int scaleIndex)
{
	return (int32_t)((int32_t)1 << (3 + scaleIndex)) + sizeof(MemoryNodeHeader) - 1;
}

void MemoryManager::listRemove(MemoryList &mm, PMemoryNodeHeader node)
{
	if (node->prev) {
		node->prev->next = node->next;
	}

	if (node->next) {
		node->next->prev = node->prev;
	}

	if (node == mm.first) {
		mm.first = node->next;
	}

	if (node == mm.last) {
		mm.last = node->prev;
	}

	node->prev = node->next = nullptr;
}

void MemoryManager::listInsertFirst(MemoryList &mm, PMemoryNodeHeader node)
{
	PMemoryNodeHeader oldFirst = (PMemoryNodeHeader)mm.first;
	mm.first = node;
	node->next = oldFirst;
	if (oldFirst) {
		oldFirst->prev = node;
	}
}

void MemoryManager::listInsertLast(MemoryList &mm, PMemoryNodeHeader node)
{
	PMemoryNodeHeader oldLast = (PMemoryNodeHeader)mm.last;
	mm.last = node;
	node->prev = oldLast;
	if (oldLast) {
		oldLast->next = node;
	}
}

void initMemoryManagers(CacheSetting *setting)
{
	int workerCount = setting->read(WORKER_THREAD_COUNT).toInt();
	g_memoryManagers = new MemoryManager[workerCount];
	for (int i = 0; i < workerCount; ++i) {
		g_memoryManagers[i].init(i);
	}

	g_rootPath = setting->read(MEMORY_ROOT_PATH).toString();
	g_arrayMemoryLimit = setting->read(ARRAY_MEMORY_LIMIT).toUInt();
	g_arrayMemoryPushLimit = setting->read(ARRAY_MEMORY_PUSH_LIMIT).toUInt();
	g_varMemoryLimit = setting->read(VAR_MEMORY_LIMIT).toUInt();
	g_varMemoryPushLimit = setting->read(VAR_MEMORY_PUSH_LIMIT).toUInt();
	g_tableMemoryLimit = setting->read(TABLE_MEMORY_LIMIT).toUInt();
	g_tableMemoryPushLimit = setting->read(TABLE_MEMORY_PUSH_LIMIT).toUInt();
	g_writeBufferDefaultMemory = setting->read(WRITE_BUFFER_DEFAULT_MEMORY).toUInt();
}
