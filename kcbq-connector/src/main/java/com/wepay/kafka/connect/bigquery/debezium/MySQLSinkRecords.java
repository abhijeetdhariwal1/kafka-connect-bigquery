package com.wepay.kafka.connect.bigquery.debezium;

import com.google.cloud.bigquery.FieldList;
import com.wepay.kafka.connect.bigquery.BigQuerySinkTask;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLSinkRecords {

//    private static final Logger logger = LoggerFactory.getLogger(MySQLSinkRecords.class);
    public MySQLSinkRecords(){

    }


//    public void modifySinkRecord(SinkRecord record){
//        logger.info("hi");
//        Struct valueSchema =  (Struct) record.value();
////        String val = String.valueOf(
////                (
////                (Struct) valueSchema.get("fields")
////                ));
//
////        logger.info(val);
//
//        logger.info("\n\n\n");
//       Schema schema =record.valueSchema();
//       List<Field> filedList =schema.fields();
//       for( int i=0; i< filedList.size();i++){
//           System.out.println("+++++++++++++++++++++++++++++\n");
//           Field currentField = filedList.get(i);
//           System.out.println("i => "+ i + "-- " +  currentField.name() +  " -- " + currentField.index() );
//           Schema currentSchema  = currentField.schema();
////           Schema currentKeySchema = currentSchema.keySchema();
////           Schema currentValueSchema= currentSchema.valueSchema();
////           List<Field> currentFields =currentSchema.fields();
//           System.out.println("currentSchema: " + currentSchema);
////           System.out.println("currentKeySchema"  + currentKeySchema);
////           System.out.println("currentValueSchema: "+ currentValueSchema);
////           System.out.println("Fields");
////           for(int j=0; j<currentFields.size();j++){
////               System.out.println(currentFields.get(i));
////           }
//
//           System.out.println(currentSchema.type());
//           System.out.println(currentSchema.isOptional());
////           Struct currentSchemaFieldList = (Struct) currentSchema;
////           System.out.println(currentSchema.fields());
////           System.out.println(currentSchema.);
////           System.out.println(currentSchema.type());
//
//           System.out.println("currentSchemaFieldList:");
////           for (int j=0;j<currentSchemaFieldList.size(); j++){
////               Field currentSchemaField  = currentSchemaFieldList.get(j);
////               System.out.println(currentSchemaField);
////           }
//
////           System.out.println(currentSchema.);
//
//
//           System.out.println("----------------------\n\n");
//       }
//
//    }

    public void test(){
        Map<String, String > mp = new HashMap<>();
         BigQuerySinkConfig config= new BigQuerySinkConfig(mp);
        boolean autoCreateTables = config.getBoolean(BigQuerySinkConfig.TABLE_CREATE_CONFIG);
    }
    public static void main(String [] args){
        MySQLSinkRecords x =new MySQLSinkRecords();
       x.test();

    }
}
