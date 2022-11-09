package com.wepay.kafka.connect.bigquery.debezium;

import java.util.List;

public class MySQLTableData {
    private MySQLTableSchema schema;
    private Object  values;

    public MySQLTableSchema getSchema() {
        return schema;
    }

    public void setSchema(MySQLTableSchema schema) {
        this.schema = schema;
    }

    public Object getValues() {
        return values;
    }

    public void setValues(Object values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "MySQLTableData{" +
                "schema=" + schema +
                ", values=" + values +
                '}';
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

    @Override
    public String toString() {
        return "MySQLTableSchema{" +
                "type='" + type + '\'' +
                ", fields=" + fields +
                '}';
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

    @Override
    public String toString() {
        return "MySQLTableFields{" +
                "name='" + name + '\'' +
                ", index=" + index +
                ", schema=" + schema +
                '}';
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

    @Override
    public String toString() {
        return "MySQLFieldSchema{" +
                "type='" + type + '\'' +
                ", optional=" + optional +
                '}';
    }
}