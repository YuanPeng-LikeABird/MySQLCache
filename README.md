This is a SQL Cache Server For All RDBMS including mysql, PostgreSQL,Oracle

1.Build
There two Directorys:MySQLCache,sdk.
MySQLCache is main code in C++, build by cmake.
In this Directory, there is a Externals Directory, which contains third party libraries MySQLCache depends on:libevent, antlr,mysqlConnector.
Server part is mainly implemented by libevent.
Antlr is used to parse mysql sql statement.
mysqlConnector is for communicate with mysql Server.
These libraries shoud be builded before MySQLCache.

Build libevent by cmake both in windows and Linux.Please see README.md in LibEvent Directory.The code is download from github, you can download the latest one.
In directory:altlr4, is code of altlr(you can download newer antlr version from github).vs proj and CMakeLists.txt for c++ is in child directory:runtime/Cpp/runtime.
MySQLConnector can be downloaded from http://dev.mysql.com/downloads for Windows. For Linux, it is preferable to install directly from OS.
In Ubuntu, do command in bash:
# apt-get install libmysqlcppconn-dev
copy cpp header installed to mysql-connnector/include, because there are difference among different versions so that compile fail.
So you can download MySQLConnector code, but in my ubuntu, there is possibilty to compile fail with reason I don't known.

After dependency are prepared, build MySQLCache code by cmake.For Windows, Visual Studio Proj(2019) is there(at least C++17)

sdk is code to deal with MySQLCache in java for backend applicaiton.TestApplication.java tell all basic usages.TestApplication is all my tests until now.
Tests is not sufficient.So welcome more clever man to join to constribute development of MySQLCache.

2.Run
MySQLCache basically work standalone, but read-write split. There are two Cache Server, one is main, while the other is write.
Two server share same code, decided by start up parameter: -readMode:false. Defualt readMode is true, set false to start up write server.
Write Server should start up after Main Server, console output :connect server means connect Main Server success.
Server has a start up setting file named Cache.ini. All contents is like that below:
###
server-mode:read     #as same as start up parameter readMode
server-thread-count:8   # count of server threads
worker-thread-count:8  #count of worker threads
array-memory-limit:1073741824  #threshold to compress of arrayMemory Pool
array-memory-push-limit:1000000000 #threshold to write disk of arrayMemory Pool
var-memory-limit:1073741824  #threshold to compress of varMemory Pool
var-memory-push-limit:1000000000 #threshold to write disk of varMemory Pool
table-memory-limit:1073741824  #threshold to compress of TableContainer
write-buffer-default-memory:1048576  #init memory of buffer for write tmp data
memory-root-path:/var/tmp/ #write disk location of memory pool
server-addr:127.0.0.1  # addr of MainServer, use for Write Server to connect to
sql-server-addr:tcp://127.0.0.1:3306,root,123456,mydb  # mysql connect info, root is username, 123456 is password, mydb is database name
###

3.End
At last, MySQLCache is a weak child now. There are too many problems on functionality and efficiency, availability to solve.
Hey, man, welcome to join.