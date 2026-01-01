cmake_minimum_required(VERSION 3.0)
project(MySQLCache)

add_compile_options(-fPIC)

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -std=c++1z")
set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -g -fvisibility=hidden")
set(CMAKE_C_FLAGS  "${CMAKE_C_FLAGS} -g -fvisibility=hidden")

include_directories(./)
include_directories(./Common)
include_directories(./Compress)
include_directories(./Memory)
include_directories(./Server)
include_directories(./SQLConnector)
include_directories(./SQLParser)
include_directories(./SQLStorage)
include_directories(./SQLTable)
include_directories(./Test)
include_directories(./Utils)
include_directories(./Externals/libevent/include)
include_directories(./Externals/libevent/include/event2)
include_directories(./Externals/antlr4/runtime/Cpp/runtime/src)
include_directories(./Externals/mysql-connector/include)

set(SOURCES
    MySQLCache.cpp
	./Common/CacheSetting.cpp
	./Common/Common.cpp
	./Common/Consts.cpp
	./Common/MyVariant.cpp
	./Common/Task.cpp
	./Common/TaskQueue.cpp
	./Compress/snappy.cc
	./Compress/snappy-c.cc
	./Compress/snappy-sinksource.cc
	./Compress/snappy-stubs-internal.cc
	./Memory/ArrayMemoryManager.cpp
	./Memory/MemoryManager.cpp
	./Memory/SwapFile.cpp
	./Memory/VarMemoryManager.cpp
	./Memory/WriteBuffer.cpp
	./Server/CacheServer.cpp
	./SQLConnector/MySQLConnector.cpp
	./SQLConnector/SQLConnectorFactory.cpp
	./SQLParser/MySQLExprListener.cpp
	./SQLParser/MySqlLexer.cpp
	./SQLParser/MySqlParser.cpp
	./SQLParser/MySqlParserBaseListener.cpp
	./SQLParser/MySqlParserListener.cpp
	./SQLStorage/ByteArray.cpp
	./SQLStorage/InputStream.cpp
	./SQLStorage/OutputStream.cpp
	./SQLStorage/SQLStorage.cpp
	./SQLTable/SQLContext.cpp
	./SQLTable/SQLGraph.cpp
	./SQLTable/SQLTable.cpp
	./SQLTable/SQLTableContainer.cpp
	./SQLTable/SQLTableIndex.cpp
	./SQLTable/SQLTableSchema.cpp
	./Test/MemoryTest.cpp
	./Utils/CacheMonitor.cpp
	./Utils/MathUtils.cpp
	./Utils/StrUtils.cpp
)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/Release/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/Release/bin)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/Release/bin)

link_directories(./Externals/libevent/lib)
link_directories(./Externals/antlr4/lib)
add_executable(MySQLCache ${SOURCES} )
target_link_libraries(MySQLCache event_core event_extra event_pthreads event)
target_link_libraries(MySQLCache mysqlcppconn antlr4-runtime dl)

set_target_properties(MySQLCache PROPERTIES COMPILE_FLAGS "-pthread")
set_target_properties(MySQLCache PROPERTIES LINK_FLAGS " -pthread  -Wl,-rpath,$ORIGIN:$ORIGIN/../lib")
set_target_properties(MySQLCache PROPERTIES INSTALL_RPATH "$ORIGIN:$ORIGIN/../lib")
