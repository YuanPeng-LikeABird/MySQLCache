#pragma once
#include <string>
#include <cstdint>
#include <iostream>
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#endif

class SwapFile
{
public:
	SwapFile(const std::string &filePath, uint64_t fileSize);
	~SwapFile();

	bool tryRead(uint64_t pos, uint64_t len);

	bool read(uint64_t pos, uint8_t *mem, uint64_t len);
	bool write(uint64_t pos, uint8_t *mem, uint64_t len);

	const std::string &path() const;

private:
	void doMap(bool write = false);

private:
	std::string m_path;
#ifdef _WIN32
	HANDLE m_file;
	HANDLE m_fileMap;
#else
	int m_file;
#endif
	uint8_t *m_dataPtr;
	uint64_t m_startPos;
	uint64_t m_endPos;
};
