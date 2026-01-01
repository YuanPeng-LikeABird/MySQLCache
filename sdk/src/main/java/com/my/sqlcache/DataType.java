package com.my.sqlcache;

public enum DataType {
    dtBoolean(1),
    dtSmallInt(2),
    dtInt(3),
    dtBigInt(4),
    dtFloat(5),
    dtDouble(6),
    dtString(7),
    dtBlob(8);

    private int value;

    private DataType(int type) {
        value = type;
    }

    public int getValue() {
        return value;
    }

    public static DataType valueOf(int type) {
        switch (type) {
            case 1:
                return dtBoolean;
            case 2:
                return dtSmallInt;
            case 3:
                return dtInt;
            case 4:
                return dtBigInt;
            case 5:
                return dtFloat;
            case 6:
                return dtDouble;
            case 7:
                return dtString;
            case 8:
                return dtBlob;
            default:
                return null;
        }
    }
}
