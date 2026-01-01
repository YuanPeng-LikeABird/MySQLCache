#include "MemoryTest.h"
#include "MemoryManager.h"
#include "VarMemoryManager.h"
#include <chrono>
#include <iostream>
#include <string>

void MemoryTest::test()
{
	uint8_t *data = MemoryManager::instantce().allocate(12);
	data[0] = 12;
	data[11] = 'a';
	MemoryManager::instantce().recycle(data);
	auto t1 = std::chrono::system_clock::now();
	// 对于频繁的创建释放场景，内存池优于普通的malloc/free
	for (int i = 0; i < 1000000; ++i) {
		uint8_t *data = MemoryManager::instantce().allocate(12);
		MemoryManager::instantce().recycle(data);
	}
	auto t2 = std::chrono::system_clock::now();
	std::cout << "malloc time: " << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << std::endl;
}

void MemoryTest::testVar()
{
	std::string value = "哈哈";
	VarData data;
	data.data = (uint8_t *)value.c_str();
	data.len = value.size();
	uint32_t id = MemoryManager::instantce().varMemory().set(data);
	value = "nihao";
	data = MemoryManager::instantce().varMemory().get(id);
	std::string newValue((char *)data.data, data.len);
	std::cout << newValue << std::endl;
	newValue = "呵呵";

	data.data = (uint8_t *)newValue.c_str();
	data.len = newValue.size();
	MemoryManager::instantce().varMemory().set(id, data);

	VarData newData = MemoryManager::instantce().varMemory().get(id);
	std::string value2((char *)data.data, data.len);
	std::cout << value2 << std::endl;

	value = "新哈哈";
	data.data = (uint8_t *)value.c_str();
	data.len = value.size();
	id = MemoryManager::instantce().varMemory().set(data);
	data = MemoryManager::instantce().varMemory().get(id);
	newValue = std::string((char *)data.data, data.len);
	std::cout << newValue << std::endl;
}

void MemoryTest::testArray()
{
	ArrayMemoryManager &arrayMemory = MemoryManager::instantce().arrayMemory();
	uint32_t id1 = arrayMemory.allocate(34);
	ArrayMemoryManager::ArrayMemoryOperator &opr = arrayMemory.memoryOperator(id1);
	opr.setInt32(0, 1234);
	opr.setInt16(4, -234);
	opr.setFloat64(6, 12.3456);
	opr.setInt64(14, 12456789100L);
	opr.setUint16(22, 234);
	opr.setUint8(24, 13);
	opr.setInt8(25, 14);
	opr.setUint32(26, 1234567);
	opr.setInt32(30, -23456789);

	uint32_t id2 = arrayMemory.allocate(16);
	ArrayMemoryManager::ArrayMemoryOperator &opr2 = arrayMemory.memoryOperator(id2);
	opr2.setFloat64(0, 1.2345678);
	opr2.setFloat64(8, -23345.688);

	ArrayMemoryManager::ArrayMemoryOperator &opr3 = arrayMemory.memoryOperator(id1);
	std::cout << opr3.getInt32(0) << std::endl;
	std::cout << opr3.getInt16(4) << std::endl;
	std::cout << opr3.getFloat64(6) << std::endl;
	std::cout << opr3.getInt64(14) << std::endl;
	std::cout << opr3.getUint16(22) << std::endl;
	int8_t v8 = opr3.getUint8(23);
	v8 = opr3.getInt8(24);
	std::cout << opr3.getUint8(24) << std::endl;
	std::cout << opr3.getInt8(25) << std::endl;
	std::cout << opr3.getUint32(26) << std::endl;
	std::cout << opr3.getInt32(30) << std::endl;

	ArrayMemoryManager::ArrayMemoryOperator &opr4 = arrayMemory.memoryOperator(id2);
	double vDouble = opr4.getFloat64(0);
	vDouble = opr4.getFloat64(8);
	std::cout << vDouble << std::endl;
}

void MemoryTest::testOverflow()
{
	ArrayMemoryManager &arrayMemory = MemoryManager::instantce().arrayMemory();
	uint32_t testId;
	uint32_t testId2;
	uint32_t testId3;
	for (int i = 0; i < 30000000; ++i) {
		uint32_t id1 = arrayMemory.allocate(1000);
		if (i == 999) {
			testId = id1;
			ArrayMemoryManager::ArrayMemoryOperator &opr = arrayMemory.memoryOperator(id1);
			opr.setInt32(0, 6666);
			opr.setFloat64(10, 66.66);
		}

		if (i == 19999999) {
			testId2 = id1;
			ArrayMemoryManager::ArrayMemoryOperator &opr = arrayMemory.memoryOperator(id1);
			opr.setInt32(0, 7777);
			opr.setUint16(4, 256);
			opr.setFloat64(12, 77.77);
		}

		if (i == 999999) {
			testId3 = id1;
			ArrayMemoryManager::ArrayMemoryOperator &opr = arrayMemory.memoryOperator(id1);
			opr.setInt32(0, 8876);
			opr.setFloat64(200, 77.77);
		}
	}
	std::cout << "used memory : " << arrayMemory.used() << std::endl;
	int32_t intValue = arrayMemory.memoryOperator(testId).getInt32(0);
	double dValue = arrayMemory.memoryOperator(testId).getFloat64(10);
	std::cout << "testID1: " << intValue << " " << dValue << std::endl;

	intValue = arrayMemory.memoryOperator(testId2).getInt32(0);
	uint16_t uint16Value = arrayMemory.memoryOperator(testId2).getUint16(4);
	dValue = arrayMemory.memoryOperator(testId2).getFloat64(12);
	std::cout << "testID2: " << intValue << " " << uint16Value << " " << dValue << std::endl;

	intValue = arrayMemory.memoryOperator(testId3).getInt32(0);
	dValue = arrayMemory.memoryOperator(testId3).getFloat64(200);
	std::cout << "testID3: " << intValue << " " << dValue << std::endl;
}
