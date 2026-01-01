#include "TaskQueue.h"
#include <chrono>

const int32_t CREATE_THREAD_INVERVAL = 1800; // second

TaskQueue::TaskQueue() :
	m_putPos(-1),
	m_getPos(-1),
	m_threadId(-1),
	m_setThread(false)
{
	for (int i = 0; i < QUEUE_SIZE; ++i) {
		m_isDone[i] = true;
	}
}

TaskQueue::~TaskQueue()
{
}
// multi thread execute
void TaskQueue::addNewTask(TaskType type, TaskData *data)
{
	std::unique_lock<std::mutex> lk(m_taskLock);
	m_putPos = (m_putPos + 1) & (QUEUE_SIZE - 1);
	m_taskList[m_putPos].type = type;
	m_taskList[m_putPos].data = reinterpret_cast<intptr_t>(data);
	m_isDone[m_putPos] = false;
	m_taskCond.notify_all();
}
void TaskQueue::batchAddNewTask(std::vector<TaskType> types, std::vector<TaskData *> datas)
{
	std::unique_lock<std::mutex> lk(m_taskLock);
	uint32_t len = types.size();
	for (int i = 0; i < len; ++i) {
		m_putPos = (m_putPos + 1) & (QUEUE_SIZE - 1);
		m_taskList[m_putPos].type = types[i];
		m_taskList[m_putPos].data = reinterpret_cast<intptr_t>(datas[i]);
		m_isDone[m_putPos] = false;
	}
	m_taskCond.notify_all();
}

Task *TaskQueue::fetchTask(int32_t threadId)
{	
	std::unique_lock<std::mutex> lk(m_taskLock);
	if (threadId != m_threadId) {
		return nullptr;
	}

	m_getPos = (m_getPos + 1) & (QUEUE_SIZE - 1);
	if (m_isDone[m_getPos]) {
		m_taskCond.wait(lk, [this] {
			return !m_isDone[m_getPos];
		});
	}

	m_isDone[m_getPos] = true;
	return &m_taskList[m_getPos];
}

int32_t TaskQueue::count()
{
	std::unique_lock<std::mutex> lk(m_taskLock);
	return m_putPos - m_getPos;
}

bool TaskQueue::setThreadId(int32_t threadId)
{
	std::unique_lock<std::mutex> lk(m_taskLock);
	auto current = std::chrono::system_clock::now();
	if (!m_setThread ||
		std::chrono::duration_cast<std::chrono::seconds>(current - m_lastSetThreadTime).count() >= CREATE_THREAD_INVERVAL)
	{
		m_setThread = true;
		m_threadId = threadId;
		m_lastSetThreadTime = current;
		return true;
	}

	return false;
	

}
