package com.my.sqlcache;

public enum SQLCacheErrorCode {
    scecNone(0),
    scecInvalidSql(1),
    scecInvalidCacheSql(2),
    scecSqlFail(3),
    scecWriteServerError(4),
    scecServerError(5);

    private int code;

    private SQLCacheErrorCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static SQLCacheErrorCode valueOf(int code) {
        switch (code) {
            case 0:
                return scecNone;
            case 1:
                return scecInvalidSql;
            case 2:
                return scecInvalidCacheSql;
            case 3:
                return scecSqlFail;
            case 4:
                return scecWriteServerError;
            case 5:
                return scecServerError;
            default:
                return null;
        }
    }
}
