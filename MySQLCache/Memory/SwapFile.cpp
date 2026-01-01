#include "SwapFile.h"
#ifndef _WIN32
#include <cstring>
#endif

#define MAP_VIEW_SIZE 1073741824

SwapFile::SwapFile(const std::string &filePath, uint64_t fileSize) :
	m_path(filePath),
	m_dataPtr(nullptr),
	m_startPos(0)
{
	m_endPos = ((fileSize - 1) / MAP_VIEW_SIZE + 1) * MAP_VIEW_SIZE;
#ifdef _WIN32
	m_file = CreateFileA(m_path.c_str(), GENERIC_READ | GENERIC_WRITE, 
		0, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
	m_fileMap = CreateFileMapping(m_file, NULL, PAGE_READWRITE, (DWORD)(m_endPos >> 32),
		(DWORD)(m_endPos & 0xFFFFFFFF), NULL);
	if (m_fileMap) {
		m_dataPtr = (uint8_t *)MapViewOfFile(m_fileMap, FILE_MAP_ALL_ACCESS, 0, 0, MAP_VIEW_SIZE);
	}
#else
	m_file = open(m_path.c_str(), O_RDWR | O_CREAT, S_IRWXU);
	if (m_file == -1) {
		std::cerr << "Error Create Swapfile" << std::endl;
		return;
	}

	if (ftruncate(m_file, m_endPos) == -1) {
		std::cerr << "Error truncating Swapfile" << std::endl;
		close(m_file);
		return;
	}

	void *data = mmap(NULL, MAP_VIEW_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, m_file, 0);
	if (data == MAP_FAILED) {
		std::cerr << "Error mmapping the file" << std::endl;
		close(m_file);
		return;
	}
	m_dataPtr = (uint8_t*)data;
#endif
}

SwapFile::~SwapFile()
{
#ifdef _WIN32
	if (m_dataPtr) {
		UnmapViewOfFile(m_dataPtr);
		m_dataPtr = nullptr;
	}
	CloseHandle(m_fileMap);
	CloseHandle(m_file);
#else
	if (m_dataPtr) {
		munmap((void *)m_dataPtr, MAP_VIEW_SIZE);
		m_dataPtr = nullptr;
	}
	close(m_file);
#endif
}

bool SwapFile::tryRead(uint64_t pos, uint64_t len)
{
	return pos + len <= m_endPos;
}

bool SwapFile::read(uint64_t pos, uint8_t *mem, uint64_t len)
{
	if (pos + len > m_endPos) {
		return false;
	}

	if (pos < m_startPos || pos - m_startPos >= MAP_VIEW_SIZE) {
		m_startPos = pos / MAP_VIEW_SIZE * MAP_VIEW_SIZE;
		doMap();
	}

	uint64_t offset = pos - m_startPos;
	if (pos + len >= m_startPos + MAP_VIEW_SIZE) {
		uint64_t prevLen = m_startPos + MAP_VIEW_SIZE - pos;
#ifdef _WIN32
		memcpy_s(mem, prevLen, m_dataPtr + offset, prevLen);
#else
		memcpy(mem, m_dataPtr + offset, prevLen);
#endif
		m_startPos += MAP_VIEW_SIZE;
		doMap();
#ifdef _WIN32
		memcpy_s(mem + prevLen, len - prevLen, m_dataPtr, len - prevLen);
#else
		memcpy(mem + prevLen, m_dataPtr, len - prevLen);
#endif
	}
	else {
#ifdef _WIN32
		memcpy_s(mem, len, m_dataPtr + offset, len);
#else
		memcpy(mem, m_dataPtr + offset, len);
#endif
	}

	return true;
}

bool SwapFile::write(uint64_t pos, uint8_t *mem, uint64_t len)
{
	if (pos + len > m_endPos) {
		return false;
	}

	if (pos < m_startPos || pos - m_startPos >= MAP_VIEW_SIZE) {
		m_startPos = pos / MAP_VIEW_SIZE * MAP_VIEW_SIZE;
		doMap(true);
	}
	
	uint64_t offset = pos - m_startPos;
	if (pos + len >= m_startPos + MAP_VIEW_SIZE) {
		uint64_t prevLen = m_startPos + MAP_VIEW_SIZE - pos;
#ifdef _WIN32
		memcpy_s(m_dataPtr + offset, prevLen, mem, prevLen);
#else
		memcpy(m_dataPtr + offset, mem, prevLen);
#endif
		m_startPos += MAP_VIEW_SIZE;
		doMap(true);
#ifdef _WIN32
		memcpy_s(m_dataPtr, len - prevLen, mem + prevLen, len - prevLen);
#else
		memcpy(m_dataPtr, mem + prevLen, len - prevLen);
#endif
	}
	else {
#ifdef _WIN32
		memcpy_s(m_dataPtr + offset, len, mem, len);
#else
		memcpy(m_dataPtr + offset, mem, len);
#endif
	}

	return true;
}

const std::string &SwapFile::path() const
{
	return m_path;
}

void SwapFile::doMap(bool write)
{
#ifdef _WIN32
	if (m_dataPtr) {
		if (write) {
			FlushViewOfFile(m_dataPtr, MAP_VIEW_SIZE);
		}
		UnmapViewOfFile(m_dataPtr);
	}
	
	m_dataPtr = (uint8_t *)MapViewOfFile(m_fileMap, FILE_MAP_ALL_ACCESS, (DWORD)(m_startPos >> 32),
		(DWORD)(m_startPos & 0xFFFFFFFF), MAP_VIEW_SIZE);
#else
	if (m_dataPtr) {
		if (write) {
			msync((void *)m_dataPtr, MAP_VIEW_SIZE, MS_SYNC);
		}
		munmap((void*)m_dataPtr, MAP_VIEW_SIZE);
	}

	m_dataPtr = (uint8_t *)mmap(NULL, MAP_VIEW_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, m_file, m_startPos);
#endif
}
