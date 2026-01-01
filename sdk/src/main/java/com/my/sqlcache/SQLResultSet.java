package com.my.sqlcache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SQLResultSet {

    private SQLResultInputStream input = null;

    private List<SQLField> fieldList = new ArrayList<>();

    private HashMap<String, Object> fieldValues = new HashMap<>();

    private int resCount = 0;

    private int resIter = -1;

    private TableKind myTableKind = TableKind.tkUnknown;
    // for join table, every element is field count of one table
    private List<Integer> fieldCounts = new ArrayList<>();

    public SQLResultSet(SQLResultInputStream inputStream) {
        this.input = inputStream;
        load();
    }

    public boolean next() {
        if (++resIter >= resCount) {
            --resIter;
            return false;
        }

        readRecord();
        return true;
    }

    public int fieldCount() {
        return fieldList.size();
    }

    public SQLField field(int index) {
        return fieldList.get(index);
    }

    public Object value(String fieldName) {
        return fieldValues.get(fieldName);
    }

    private void load() {
        loadSchema();
        resCount = input.readInt();
        resIter = -1;
    }

    private void loadSchema() {
        TableKind tblKind = TableKind.valueOf(input.readByte());
        if (myTableKind == TableKind.tkUnknown) {
            myTableKind = tblKind;
        }

        if (tblKind == TableKind.tkJoin) {
            loadSchema();
            loadSchema();
        }
        else {
            input.readString();
            int fieldCnt = input.readShort();
            if (myTableKind == TableKind.tkJoin) {
                fieldCounts.add(fieldCnt);
            }

            for (int i = 0; i < fieldCnt; ++i) {
                SQLField field = new SQLField();
                field.setName(input.readString());
                field.setDataType(DataType.valueOf(input.readByte()));
                field.setQuery(input.readBoolean());
                field.setPrimary(input.readBoolean());
                if (field.isQuery()) {
                    field.setAliasName(input.readString());
                }
                fieldList.add(field);
            }
        }
    }

    private void readRecord() {
        fieldValues.clear();
        if (myTableKind == TableKind.tkJoin) {
            int start = 0;
            for (int i = 0; i < fieldCounts.size(); ++i) {
                readRecordFromTable(start, start + fieldCounts.get(i));
                start += fieldCounts.get(i);
            }
        }
        else {
            readRecordFromTable(0, fieldList.size());
        }

    }

    private void readRecordFromTable(int startFieldIndex, int endFieldIndex) {
        int nullByteCount = (endFieldIndex - startFieldIndex - 1) / 8 + 1;
        byte[] nullBytes = new byte[nullByteCount];
        for (int i = 0; i < nullByteCount; ++i) {
            nullBytes[i] = input.readByte();
        }

        int nullByteIndex = 0;
        int nullBitIndex = 0;
        for (int i = startFieldIndex; i < endFieldIndex; ++i) {
            SQLField field = fieldList.get(i);
            String name = field.getName();
            boolean isNull = false;
            if ((nullBytes[nullByteIndex] & ((byte)1 << nullBitIndex++)) != 0) {
                fieldValues.put(name, null);
                isNull = true;
            }

            if (nullBitIndex == 8) {
                nullBitIndex = 0;
            }

            switch (field.dataType()) {
                case dtBoolean: {
                    boolean value = input.readBoolean();
                    if (!isNull) {
                        fieldValues.put(name, value);
                    }
                    break;
                }

                case dtSmallInt: {
                    short value = input.readShort();
                    if (!isNull) {
                        fieldValues.put(name, value);
                    }
                    break;
                }

                case dtInt: {
                    int value = input.readInt();
                    if (!isNull) {
                        fieldValues.put(name, value);
                    }
                    break;
                }

                case dtBigInt: {
                    long value = input.readLong();
                    if (!isNull) {
                        fieldValues.put(name, value);
                    }
                    break;
                }

                case dtFloat: {
                    float value = input.readFloat();
                    if (!isNull) {
                        fieldValues.put(name, value);
                    }
                    break;
                }

                case dtDouble: {
                    double value = input.readDouble();
                    if (!isNull) {
                        fieldValues.put(name, value);
                    }
                    break;
                }

                case dtString: {
                    String value = input.readString();
                    if (!isNull) {
                        fieldValues.put(name, value);
                    }
                    break;
                }

                case dtBlob: {
                    byte[] value = input.readBlock();
                    if (!isNull) {
                        fieldValues.put(name, value);
                    }
                    break;
                }
            }
        }
    }
}
