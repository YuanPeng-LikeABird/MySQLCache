package com.my.sqlcache;

public class TestApplication {

    public static void main(String[] args) {
        SQLCacheConnectorPool connectorPool = new SQLCacheConnectorPool();
        try {
            connectorPool.startup("127.0.0.1", 9000);
            SQLCacheConnector connector = connectorPool.get();
            test1(connector);
            test2(connector);
            test3(connector);
            test4(connector);
            String info = connector.monitor();
            System.out.println(info);
            connectorPool.free(connector);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connectorPool.shutDown();
        }
    }

    public static void test1(SQLCacheConnector connector) throws Exception {
        SQLResultSet result = connector.select("SELECT * FROM student WHERE score > ? ORDER BY score DESC;", ParamDataType.pdtDouble, 123);
        printResult(result);

       /* result = connector.select("SELECT name, Sum(score) AS score, Sum(id) AS IDCount FROM student GROUP BY name HAVING name <> ? AND sum(score) > ?;", ParamDataType.pdtString, "AA", ParamDataType.pdtDouble, 100);
        printResult(result);*/

        connector.update("INSERT INTO student(score, name) VALUES(?, ?);", ParamDataType.pdtDouble, 123, ParamDataType.pdtString, "bb");

        result = connector.select("SELECT * FROM student WHERE score > ? ORDER BY score DESC;", ParamDataType.pdtDouble, 123);
        printResult(result);

        connector.update("UPDATE student SET score = ? WHERE name = ?;", ParamDataType.pdtDouble, 246, ParamDataType.pdtString, "bb");

        result = connector.select("SELECT * FROM student WHERE score > ? ORDER BY score DESC;", ParamDataType.pdtDouble, 123);
        printResult(result);

        connector.update("DELETE FROM student WHERE name = ?;", ParamDataType.pdtString, "bb");

        result = connector.select("SELECT * FROM student WHERE score > ? ORDER BY score DESC;", ParamDataType.pdtDouble, 123);
        printResult(result);
    }

    public static void test2(SQLCacheConnector connector) throws Exception {
        SQLResultSet result = connector.select("SELECT b.description, a.name AS aName FROM teacher a JOIN subject b ON a.subjectID = b.subjectID");
        printResult(result);

        result = connector.select("SELECT b.description, Sum(teacherID) AS teacherCount FROM teacher a JOIN subject b ON a.subjectID = b.subjectID GROUP BY description");
        printResult(result);

        connector.update("INSERT INTO teacher(name, subjectID) values(?, ?), (?, ?);", ParamDataType.pdtString, "yuan", ParamDataType.pdtInt, 2, ParamDataType.pdtString, "peng", ParamDataType.pdtInt, 3);
        result = connector.select("SELECT b.description, Sum(teacherID) AS teacherCount FROM teacher a JOIN subject b ON a.subjectID = b.subjectID GROUP BY description");
        printResult(result);

        connector.update("UPDATE teacher set subjectID = ? WHERE name = ?;", ParamDataType.pdtInt, 3, ParamDataType.pdtString, "yuan");
        result = connector.select("SELECT b.description, Sum(teacherID) AS teacherCount FROM teacher a JOIN subject b ON a.subjectID = b.subjectID GROUP BY description");
        printResult(result);

        connector.update("DELETE FROM teacher WHERE name = ? OR name = ?;", ParamDataType.pdtString, "yuan", ParamDataType.pdtString, "peng");
        result = connector.select("SELECT b.description, Sum(teacherID) AS teacherCount FROM teacher a JOIN subject b ON a.subjectID = b.subjectID GROUP BY b.description");
        printResult(result);
    }

    public static void test3(SQLCacheConnector connector) throws Exception {
        SQLResultSet result = connector.select("SELECT b.description, a.name AS aName FROM teacher a, subject b WHERE a.subjectID = b.subjectID");
        printResult(result);

        result = connector.select("SELECT b.description, Sum(teacherID) AS teacherCount FROM teacher a, subject b WHERE a.subjectID = b.subjectID GROUP BY description");
        printResult(result);

        connector.update("INSERT INTO teacher(name, subjectID) values(?, ?), (?, ?);", ParamDataType.pdtString, "yuan", ParamDataType.pdtInt, 2, ParamDataType.pdtString, "peng", ParamDataType.pdtInt, 3);
        result = connector.select("SELECT b.description, Sum(teacherID) AS teacherCount FROM teacher a, subject b WHERE a.subjectID = b.subjectID GROUP BY description");
        printResult(result);

        connector.update("UPDATE teacher set subjectID = ? WHERE name = ?;", ParamDataType.pdtInt, 3, ParamDataType.pdtString, "yuan");
        result = connector.select("SELECT b.description, Sum(teacherID) AS teacherCount FROM teacher a, subject b WHERE a.subjectID = b.subjectID GROUP BY description");
        printResult(result);

        connector.update("DELETE FROM teacher WHERE name = ? OR name = ?;", ParamDataType.pdtString, "yuan", ParamDataType.pdtString, "peng");
        result = connector.select("SELECT b.description, Sum(teacherID) AS teacherCount FROM teacher a, subject b WHERE a.subjectID = b.subjectID GROUP BY description");
        printResult(result);
    }

    public static void test4(SQLCacheConnector connector) throws Exception {
        SQLResultSet result = connector.select("SELECT b.description, a.name AS aName FROM teacher a JOIN subject b ON a.subjectID = b.subjectID");
        printResult(result);

        connector.update("INSERT INTO teacher(name, subjectID) values(?, ?), (?, ?);", ParamDataType.pdtString, "yuan", ParamDataType.pdtInt, 2, ParamDataType.pdtString, "peng", ParamDataType.pdtInt, 3);
        result = connector.select("SELECT b.description, a.name AS aName FROM teacher a JOIN subject b ON a.subjectID = b.subjectID");
        printResult(result);

        connector.update("UPDATE teacher set subjectID = ? WHERE name = ?;", ParamDataType.pdtInt, 3, ParamDataType.pdtString, "yuan");
        result = connector.select("SELECT b.description, a.name AS aName FROM teacher a JOIN subject b ON a.subjectID = b.subjectID");
        printResult(result);

        connector.update("DELETE FROM teacher WHERE name = ? OR name = ?;", ParamDataType.pdtString, "yuan", ParamDataType.pdtString, "peng");
        result = connector.select("SELECT b.description, a.name AS aName FROM teacher a JOIN subject b ON a.subjectID = b.subjectID");
        printResult(result);
    }

    public static void printResult(SQLResultSet result) {
        if (result == null) {
            return;
        }

        while(result.next()) {
            System.out.println();
            for (int i = 0; i < result.fieldCount(); ++i) {
                if (!result.field(i).isQuery()) {
                    continue;
                }

                String fieldName = result.field(i).getName();
                String queryName = result.field(i).queryName();
                System.out.print(String.format("%s %s ", queryName, result.value(fieldName)));
            }
        }
        System.out.println("\n**************************************");
    }
}
