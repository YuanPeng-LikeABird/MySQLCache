package com.my.sqlcache;

public enum ParamDataType {
    pdtString(1),
    pdtBool(2),
    pdtInt(3),
    pdtLong(4),
    pdtDouble(5),
    pdtBlob(6);

    private int value;

    ParamDataType(int type) {
        value = type;
    }

    public int getValue() {
        return value;
    }

    public static ParamDataType valueOf(int type) {
        switch (type) {
            case 1:
                return pdtString;
            case 2:
                return pdtBool;
            case 3:
                return pdtInt;
            case 4:
                return pdtLong;
            case 5:
                return pdtDouble;
            case 6:
                return pdtBlob;
            default:
                return null;
        }
    }
}
