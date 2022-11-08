package com.wepay.kafka.connect.bigquery.debezium;

import java.util.List;

public class MySQLTableData {
    private Object schema;
    private MySQLTableSchema  values;

    public Object getSchema() {
        return schema;
    }

    public void setSchema(Object schema) {
        this.schema = schema;
    }

    public MySQLTableSchema getValues() {
        return values;
    }

    public void setValues(MySQLTableSchema values) {
        this.values = values;
    }
}

class MySQLTableSchema{
    private String type;
    private List<MySQLTableFields> fields;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<MySQLTableFields> getFields() {
        return fields;
    }

    public void setFields(List<MySQLTableFields> fields) {
        this.fields = fields;
    }
}

class MySQLTableFields{
    private String name;
    private int index;
    private MySQLFieldSchema schema;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public MySQLFieldSchema getSchema() {
        return schema;
    }

    public void setSchema(MySQLFieldSchema schema) {
        this.schema = schema;
    }
}

class MySQLFieldSchema{
    private String type;
    private boolean optional;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isOptional() {
        return optional;
    }

    public void setOptional(boolean optional) {
        this.optional = optional;
    }
}