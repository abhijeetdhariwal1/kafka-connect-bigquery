package com.wepay.kafka.connect.bigquery.debezium;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.EncryptionConfiguration;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

public class BigQueryAPI {

    public void createBqTable(){
        String datasetName = "MY_DATASET_NAME";
        String tableName = "MY_TABLE_NAME";
        String kmsKeyName = "MY_KEY_NAME";
        Schema schema =
                Schema.of(
                        Field.of("stringField", StandardSQLTypeName.STRING),
                        Field.of("booleanField", StandardSQLTypeName.BOOL));
        // i.e. projects/{project}/locations/{location}/keyRings/{key_ring}/cryptoKeys/{cryptoKey}
        EncryptionConfiguration encryption =
                EncryptionConfiguration.newBuilder().setKmsKeyName(kmsKeyName).build();
        createTableCmek(datasetName, tableName, schema, encryption);
    }

    public static void createTableCmek(
            String datasetName, String tableName, Schema schema, EncryptionConfiguration configuration) {
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            TableId tableId = TableId.of(datasetName, tableName);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo =
                    TableInfo.newBuilder(tableId, tableDefinition)
                            .setEncryptionConfiguration(configuration)
                            .build();

            bigquery.create(tableInfo);
            System.out.println("Table cmek created successfully");
        } catch (BigQueryException e) {
            System.out.println("Table cmek was not created. \n" + e.toString());
        }
    }
}
