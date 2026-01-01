#pragma once

#include "VarMemoryManager.h"
#include "ArrayMemoryManager.h"
#include "CacheSetting.h"
#include "TaskQueue.h"
#include "Task.h"
#include "WriteBuffer.h"
#include <cstdint>

/*
   node的内存布局
** prevNode的地址
** nextNode的地址
** 1个字节表示内存大小
** 数据
*/
#pragma pack(1)  
typedef struct MemoryNodeHeader
{
	struct MemoryNodeHeader *prev;
	struct MemoryNodeHeader *next;
	uint8_t size;
} *PMemoryNodeHeader;
#pragma pack()  

typedef struct MemoryBlockHeader
{
	struct MemoryBlockHeader *next;
} *PMemoryBlockHeader;

// 从first拿空闲内存，然后转移到last,从而first永远是空闲内存
typedef struct MemoryList
{
	PMemoryNodeHeader first = nullptr;
	PMemoryNodeHeader last = nullptr;
	PMemoryBlockHeader firstBlock = nullptr;
	PMemoryBlockHeader lastBlock = nullptr;
	int32_t total = 0;
	int32_t idle = 0;
} *PMemoryList;

void initMemoryManagers(CacheSetting *setting);

class MemoryManager
{
public:
	MemoryManager();
	~MemoryManager();

	static MemoryManager &instantce(int index = 0);

	static std::string rootPath();
	static uint32_t arrayMemoryLimit();
	static uint32_t arrayMemoryPushLimit();
	static uint32_t varMemoryLimit();
	static uint32_t varMemoryPushLimit();
	static uint32_t tableMemoryLimit();
	static uint32_t tableMemoryPushLimit();
	static uint32_t writeBufferDefaultMemory();

	void init(int index);

	uint32_t regularMemoryLimit() const;

	uint8_t *allocate(uint32_t size);
	void recycle(uint8_t *mem);

	VarMemoryManager &varMemory();
	ArrayMemoryManager &arrayMemory();
	WriteBuffer& buffer();

	uint8_t index() const;

	TaskQueue *taskQueue() const;
	void setTaskQueue(TaskQueue *queue);

private:
	void freeNode(MemoryList &mm, PMemoryNodeHeader node);
	void recycleNode(MemoryList &mm, PMemoryNodeHeader node);
	// 在first位置预分配内存，一个block，scaleIndex is the index of m_mms
	uint8_t *prepare(int scaleIndex);
	int32_t nodeSize(int scaleIndex);

	void listRemove(MemoryList &mm, PMemoryNodeHeader node);
	void listInsertFirst(MemoryList &mm, PMemoryNodeHeader node);
	void listInsertLast(MemoryList &mm, PMemoryNodeHeader node);


private:
	uint8_t m_index;
	// 按照2^3字节，2^4字节，2^5字节..依次排列，最大2^16(65536字节),超过则放入最后一个memoryList
	PMemoryList m_mms;
	VarMemoryManager m_varMemory;
	ArrayMemoryManager m_arrayMemory;
	WriteBuffer m_buffer;
	TaskQueue *m_taskQueue;
};
