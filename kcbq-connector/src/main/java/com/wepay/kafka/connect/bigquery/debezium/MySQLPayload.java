package com.wepay.kafka.connect.bigquery.debezium;

import org.apache.kafka.common.protocol.types.Field;

import java.util.List;

public class MySQLPayload {
    private MySQLPayloadSchema schema;
    private List<Object> values;

    public MySQLPayloadSchema getSchema() {
        return schema;
    }

    public void setSchema(MySQLPayloadSchema schema) {
        this.schema = schema;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "MySQLPayload{" +
                "schema=" + schema +
                ", values=" + values +
                '}';
    }
}

class MySQLPayloadSchema {
//    private List<Object> fields;
//
//    public List<Object> getFields() {
//        return fields;
//    }
//
//    public void setFields(List<Object> fields) {
//        this.fields = fields;
//    }

    private List<MYSQLFields> fields;

    public List<MYSQLFields> getFields() {
        return fields;
    }

    public void setFields(List<MYSQLFields> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "MySQLPayloadSchema{" +
                "fields=" + fields +
                '}';
    }
}

class MYSQLFields{
    private String name;
    private int index;

    private FieldsSchema schema;

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

    @Override
    public String toString() {
        return "MYSQLFields{" +
                "name='" + name + '\'' +
                ", index=" + index +
                ", schema=" + schema +
                '}';
    }
}


class FieldsSchema{
    private List<TableFields> fields;

    public List<TableFields> getFields() {
        return fields;
    }

    public void setFields(List<TableFields> fields) {
        this.fields = fields;
    }

    @Override
    public String toString() {
        return "FieldsSchema{" +
                "fields=" + fields +
                '}';
    }
}

class TableFields{
    private String name;
    private int index;
    private TableSchema schema;

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

    public Object getSchema() {
        return schema;
    }

    public void setSchema(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        return "TableFields{" +
                "name='" + name + '\'' +
                ", index=" + index +
                ", schema=" + schema +
                '}';
    }
}

class TableSchema{
    private List<FieldSchema> fields;

    @Override
    public String toString() {
        return "TableSchema{" +
                "fields=" + fields +
                '}';
    }
}

class FieldSchema{
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
        return "FieldSchema{" +
                "type='" + type + '\'' +
                ", optional=" + optional +
                '}';
    }
}