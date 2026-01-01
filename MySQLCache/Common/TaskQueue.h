#pragma once

#include "Task.h"
#include <mutex>
#include <condition_variable>
#include <chrono>

#define QUEUE_SIZE 262144

class TaskQueue
{
public:
    TaskQueue();
    ~TaskQueue();

    void addNewTask(TaskType type, TaskData *data);
    void batchAddNewTask(std::vector<TaskType> types, std::vector<TaskData *> datas);
    Task *fetchTask(int32_t threadId);
    int32_t count();

    bool setThreadId(int32_t threadId);

private:
    Task m_taskList[QUEUE_SIZE];
    bool m_isDone[QUEUE_SIZE];
    // 新添加任务位置
    int64_t m_putPos;
    // 新处理完成任务位置
    int64_t m_getPos;

    std::mutex m_taskLock;
    std::condition_variable m_taskCond;
    int32_t m_threadId;
    bool m_setThread;
    std::chrono::system_clock::time_point m_lastSetThreadTime;
};
