#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include <string>
#include <atomic>
#include "ByteArray.h"
#include "Common.h"
#include "WriteBuffer.h"

enum class TaskType
{
    ttUnknown = 0,
    ttInsert = 1,
    ttUpdate = 2,
    ttDelete = 3,
    ttSelect = 4,
    ttTransaction = 5,
    ttUpdateCache = 6,
    ttPushBlock = 7,
    ttReset = 8,
    ttFreeUpdateCacheTask = 9
};

enum class UpdateOperation
{
    umModify = 0,
    umInsert = 1,
    umDelete = 2,
    umAll = 3
};

#pragma pack(1)
struct Task
{
    TaskType type = TaskType::ttUnknown;
    intptr_t data = 0;
};
#pragma pack()

struct TaskData
{
    bool isFinish = false;
    int8_t errorCode = SQLCacheErrorCode::scecNone;
};

struct SelectTaskData : public TaskData
{
    ByteArray sqlBytes;
    WriteBuffer *buffer;
};

struct WriteTaskData : public TaskData
{
    ByteArray sqlBytes;
	int updateCount = 0;
    ByteArray extInfo;
    WriteBuffer* buffer;
};

struct UpdateCacheTaskData : public TaskData
{
    ByteArray rawData;
    UpdateOperation updateMode;
    intptr_t updateRecords = 0;
    intptr_t updateFields = 0;
    std::atomic<uint32_t> referCount;
    uint8_t lockIndex;
    uint8_t threadIndex;
};

struct PushBlockTaskData : public TaskData
{
    uint32_t start;
    uint32_t total;
    uint8_t type;
};
