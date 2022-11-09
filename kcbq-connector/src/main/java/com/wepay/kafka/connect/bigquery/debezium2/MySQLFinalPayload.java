package com.wepay.kafka.connect.bigquery.debezium2;

public class MySQLFinalPayload {
    public String name;
    public Integer index;
    public MySQLFieldSchema schema;
    public Object value;

    @Override
    public String toString() {
        return "MySQLFinalPayload{" +
                "name='" + name + '\'' +
                ", index=" + index +
                ", schema=" + schema +
                ", value=" + value +
                '}';
    }
}

class MySQLFieldSchema{
    public String type;
    public Boolean optional;

    @Override
    public String toString() {
        return "MySQLFieldScheama{" +
                "type='" + type + '\'' +
                ", optional=" + optional +
                '}';
    }
}

