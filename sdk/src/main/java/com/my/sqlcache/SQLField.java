package com.my.sqlcache;

public class SQLField {

    private String name = "";
    private String aliasName = "";
    private DataType dataType = null;
    private boolean isQuery = false;
    private boolean isPrimary = false;

    public SQLField() {
    }

    public String queryName() {
        if (this.aliasName != "") {
            return this.aliasName;
        }

        return this.name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DataType dataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public boolean isQuery() {
        return isQuery;
    }

    public void setQuery(boolean query) {
        this.isQuery = query;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setPrimary(boolean primary) {
        this.isPrimary = primary;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }
}
