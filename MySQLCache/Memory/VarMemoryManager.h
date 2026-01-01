#pragma once
#include <cstdint>
#include <vector>
#include <unordered_map>
#include "SwapFile.h"
#include "Task.h"

/*
* VarMemoryManager用于高效管理字符串,二进制数据,每个数据有一个id唯一标识
* 内存管理的粒度为一个block,每个block长度固定可配置，默认为65536字节(对应MemoryManager最大内存粒度)
* 超过65536字节的Var数据，用一个独立内存块存储，不受block格式限制
* 为快速查找id对应的数据存储位置，block集合和block内部Var数据按照id大小顺序排列，所以查找采用二分查找
* block开端4个字节存储第一个Var数据的id,其他Var数据的id可以根据VarCount和VarLength快速定位到(其他VarId
* 不需记录)
* 修改VarData时,local write,并且移动后面的VarData保证无空隙,但超过block大小,则需要从本block中删除,
* 在新block中存新值,id也发生改变，顺序增加
*/

class MemoryManager;

#pragma pack(4)  
struct VarData
{
	VarData() :
		data(nullptr),
		len(0) {
	}

	VarData(uint8_t *_data, uint32_t _len) :
		data(_data), 
		len(_len) {
	}

	uint8_t *data;
	uint32_t len;
};
#pragma pack()  

class VarMemoryManager
{
public:
	VarMemoryManager(MemoryManager *owner);
	~VarMemoryManager();

	void initialize();
	void reset();
	void pushBlock(PushBlockTaskData *data);

	uint32_t set(VarData data);
	uint32_t set(uint32_t id, VarData data);
	void clear(uint32_t id);

	VarData get(uint32_t id);

private:
	void assureNewBlock();
	void compressBlock();
	void uncompressBlock(uint32_t index);

	void preparePushBlock();
	void pullBlock(uint32_t index);

	bool moveReadPosToId(uint32_t id);
	VarData read(uint32_t id);
	uint32_t write(uint32_t id, VarData data);

private:
	uint32_t m_blockByteCount;

	MemoryManager *m_allocator;
	SwapFile *m_swap;

	std::vector<uint8_t *> m_blocks;
	std::unordered_map<uint32_t, uint8_t *> m_bigVars;
	// read/write cursor,read for get, write for set, while read cursor pos is random, write curpor is always backward
	uint32_t m_readPos;
	uint32_t m_writePos;
	uint8_t *m_headCursor;
	uint8_t *m_dataCursor;
	uint32_t m_maxId;
	uint32_t m_maxBigId;
	uint64_t m_used;
	bool m_inPush;
};
