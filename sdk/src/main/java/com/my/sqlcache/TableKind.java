package com.my.sqlcache;

public enum TableKind {
    tkNormal(0),
    tkJoin(1),
    tkUnknown(-1);

    private int value;

    private TableKind(int kind) {
        value = kind;
    }

    public int getValue() {
        return value;
    }

    public static TableKind valueOf(int kind) {
        switch (kind) {
            case 0:
                return tkNormal;
            case 1:
                return tkJoin;
            default:
                return tkUnknown;
        }
    }
}
