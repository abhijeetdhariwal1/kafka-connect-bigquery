/*
 * Copyright 2020 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.bigquery.*;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.debezium.MySQLPayload;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

import com.wepay.kafka.connect.bigquery.exception.ExpectedInterruptException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;

import netscape.javascript.JSObject;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * A {@link BigQueryWriter} capable of updating BigQuery table schemas and creating non-existed tables automatically.
 */
public class AdaptiveBigQueryWriter extends BigQueryWriter {
  private static final Logger logger = LoggerFactory.getLogger(AdaptiveBigQueryWriter.class);

  // The maximum number of retries we will attempt to write rows after creating a table or updating a BQ table schema.
  private static final int RETRY_LIMIT = 30;
  // Wait for about 30s between each retry to avoid hammering BigQuery with requests
  private static final int RETRY_WAIT_TIME = 30000;

  private final BigQuery bigQuery;
  private final SchemaManager schemaManager;
  private final boolean autoCreateTables;

  /**
   * @param bigQuery Used to send write requests to BigQuery.
   * @param schemaManager Used to update BigQuery tables.
   * @param retry How many retries to make in the event of a 500/503 error.
   * @param retryWait How long to wait in between retries.
   * @param autoCreateTables Whether tables should be automatically created
   */
  public AdaptiveBigQueryWriter(BigQuery bigQuery,
                                SchemaManager schemaManager,
                                int retry,
                                long retryWait,
                                boolean autoCreateTables) {
    super(retry, retryWait);
    this.bigQuery = bigQuery;
    this.schemaManager = schemaManager;
    this.autoCreateTables = autoCreateTables;
  }

  /**
   * Sends the request to BigQuery, then checks the response to see if any errors have occurred. If
   * any have, and all errors can be blamed upon invalid columns in the rows sent, attempts to
   * update the schema of the table in BigQuery and then performs the same write request.
   * @see BigQueryWriter#performWriteRequest(PartitionedTableId, SortedMap)
   */
  @Override
  public Map<Long, List<BigQueryError>> performWriteRequest(
          PartitionedTableId tableId,
          SortedMap<SinkRecord, InsertAllRequest.RowToInsert> rows) {
    InsertAllResponse writeResponse = null;
    InsertAllRequest request = null;

    try {
      request = createInsertAllRequest(tableId, rows.values());
      writeResponse = bigQuery.insertAll(request);
      // Should only perform one schema update attempt.
      if (writeResponse.hasErrors()
              && onlyContainsInvalidSchemaErrors(writeResponse.getInsertErrors())) {
        attemptSchemaUpdate(tableId, new ArrayList<>(rows.keySet()));
      }
    } catch (BigQueryException exception) {
      // Should only perform one table creation attempt.
      if (BigQueryErrorResponses.isNonExistentTableError(exception) && autoCreateTables) {
        attemptTableCreate(tableId.getBaseTableId(), new ArrayList<>(rows.keySet()));
      } else if (BigQueryErrorResponses.isTableMissingSchemaError(exception)) {
        attemptSchemaUpdate(tableId, new ArrayList<>(rows.keySet()));
      } else {
        throw exception;
      }

      throw exception;
    }

    // Creating tables or updating table schemas in BigQuery takes up to 2~3 minutes to take affect,
    // so multiple insertion attempts may be necessary.
    int attemptCount = 0;
    while (writeResponse == null || writeResponse.hasErrors()) {
      logger.trace("insertion failed");
      if (writeResponse == null
          || onlyContainsInvalidSchemaErrors(writeResponse.getInsertErrors())) {
        try {
          // If the table was missing its schema, we never received a writeResponse
          logger.debug("re-attempting insertion");
          writeResponse = bigQuery.insertAll(request);
        } catch (BigQueryException exception) {
          if ((BigQueryErrorResponses.isNonExistentTableError(exception) && autoCreateTables)
              || BigQueryErrorResponses.isTableMissingSchemaError(exception)
          ) {
            // no-op, we want to keep retrying the insert
            logger.debug("insertion failed", exception);
          } else {
            throw exception;
          }
        }
      } else {
        return writeResponse.getInsertErrors();
      }
      attemptCount++;
      if (attemptCount >= RETRY_LIMIT) {
        throw new BigQueryConnectException(
            "Failed to write rows after BQ table creation or schema update within "
                + RETRY_LIMIT + " attempts for: " + tableId.getBaseTableId());
      }
      try {
        Thread.sleep(RETRY_WAIT_TIME);
      } catch (InterruptedException e) {
        throw new ExpectedInterruptException("Interrupted while waiting to retry write");
      }
    }
    logger.debug("table insertion completed successfully");
    return new HashMap<>();
  }

  protected void attemptSchemaUpdate(PartitionedTableId tableId, List<SinkRecord> records) {
    try {
      schemaManager.updateSchema(tableId.getBaseTableId(), records);
    } catch (BigQueryException exception) {
      throw new BigQueryConnectException(
          "Failed to update table schema for: " + tableId.getBaseTableId(), exception);
    }
  }

  protected void attemptTableCreate(TableId tableId, List<SinkRecord> records) {
    try {
      schemaManager.createTable(tableId, records);
    } catch (BigQueryException exception) {
      throw new BigQueryConnectException(
              "Failed to create table " + tableId, exception);
    }
  }

  /*
   * Currently, the only way to determine the cause of an insert all failure is by examining the map
   * object returned by the insertErrors() method of an insert all response. The only way to
   * determine the cause of each individual error is by manually examining each error's reason() and
   * message() strings, and guessing what they mean. Ultimately, the goal of this method is to
   * return whether or not an insertion failed due solely to a mismatch between the schemas of the
   * inserted rows and the schema of the actual BigQuery table.
   * This is why we can't have nice things, Google.
   */
  private boolean onlyContainsInvalidSchemaErrors(Map<Long, List<BigQueryError>> errors) {
    logger.trace("write response contained errors: \n{}", errors);
    boolean invalidSchemaError = false;
    for (List<BigQueryError> errorList : errors.values()) {
      for (BigQueryError error : errorList) {
        if (BigQueryErrorResponses.isMissingRequiredFieldError(error) || BigQueryErrorResponses.isUnrecognizedFieldError(error)) {
          invalidSchemaError = true;
        } else if (!BigQueryErrorResponses.isStoppedError(error)) {
          /* if some rows are in the old schema format, and others aren't, the old schema
           * formatted rows will show up as error: stopped. We still want to continue if this is
           * the case, because these errors don't represent a unique error if there are also
           * invalidSchemaErrors.
           */
          return false;
        }
      }
    }
    // if we only saw "stopped" errors, we want to return false. (otherwise, return true)
    return invalidSchemaError;
  }


  public Map<Long, List<BigQueryError>> performWriteRequest2(
          PartitionedTableId tableId,
          SortedMap<SinkRecord, InsertAllRequest.RowToInsert> rows) {
    InsertAllResponse writeResponse = null;
    InsertAllRequest request = null;

    try {

      List<SinkRecord> sinkRecordList = new ArrayList<>(rows.keySet());
//      SinkRecord  sr1= sinkRecordList.get(0);
      Gson gson = new Gson();
//      String sr1json = gson.toJson(sr1);
      String sinkRecordListJson = gson.toJson(sinkRecordList);

//      logger.info("sinkRecordList");
//      logger.info(sinkRecordListJson);

//      logger.info("sr1json");
//      logger.info(sr1json);

      MySQLPayload mySQLPayloadObj =parseMySQlSinkRecords(sinkRecordList);
      request = createInsertAllRequest(tableId, rows.values());

      Schema schema =
              Schema.of(
                      Field.of("stringField", StandardSQLTypeName.STRING),
                      Field.of("booleanField", StandardSQLTypeName.BOOL));
      String datasetName = "testdataset1";
      String tableName =  "testtable7";
      TableId myTableId = TableId.of(datasetName, tableName);
      TableDefinition tableDefinition = StandardTableDefinition.of(schema);
      TableInfo tableInfo =
              TableInfo.newBuilder(myTableId, tableDefinition)
                      .build();

      logger.info("printing MYSQL PAYLOAD");
      System.out.println(mySQLPayloadObj);
//      bigQuery.create(tableInfo);

//      logger.info("***********************");
////      int col1= records.get(0).getAsInt();
////      String varchar_col= records.get(1).getAsString();
//      System.out.println();
//      Map<String, Object> rowContent1 = new HashMap<>();
//      rowContent1.put("col1", 1);
//      rowContent1.put("varchar_col", "abc");
////      Map<String, Object> rowContent2 = new HashMap<>();
////      rowContent2.put("stringField", );
////      rowContent2.put("booleanField", false);
//
//
//      InsertAllRequest insertAllRequest =InsertAllRequest.newBuilder(TableId.of(datasetName, tableName))
//              .setRows(
//
//                                                            ImmutableList.of(
//                                              InsertAllRequest.RowToInsert.of(rowContent1)
////                                              ,InsertAllRequest.RowToInsert.of(rowContent2)
//                                                            )
//              )
//              .build();
//
//      InsertAllResponse response=  bigQuery.insertAll(insertAllRequest);
//      logger.info("response is ");
//      logger.info(response.toString());


    } catch (BigQueryException exception) {
      System.out.println("exception");
      throw exception;
    }

//    check orignal method before final commit
    return new HashMap<>();
  }

  private MySQLPayload parseMySQlSinkRecords(List<SinkRecord> sinkRecordList) {
    for(SinkRecord sinkRecord : sinkRecordList){

      try {
//        Struct data = (Struct) sinkRecord.value();
//        logger.info(data.toString());
//        String val = String.valueOf(((Struct) data.get("after")).get("SourceInvId")).trim();
//        val = (val.equals("null") || val.equals("")) ? "0" : val;
        Gson gson = new Gson();
        String  sinkRecordJson = gson.toJson(sinkRecord);
        logger.info(sinkRecordJson);
        JsonObject convertedObject = new Gson().fromJson(sinkRecordJson, JsonObject.class);
        JsonObject valueObject = convertedObject.getAsJsonObject("value");
        MySQLPayload mySQLPayloadObj = gson.fromJson(valueObject.toString(), MySQLPayload.class);
        return mySQLPayloadObj;
//        logger.info("object payload printing");
//        logger.info("printing the schema");
//        logger.info("printing the schema again");
//        logger.info("printing the schema again2");
//        logger.info("printing the schema again3");
//        logger.info(obj.toString());
//        System.out.println(obj.getValues());
//        System.out.println(obj.getSchema());
//        System.out.println(obj.getValues().size());
//        JsonObject schemaObject = valueObject.getAsJsonObject("schema");
//        System.out.println("__________________________________________________");
//
//        JsonArray fieldsValueObject = schemaObject.getAsJsonArray("fields");
//
//        JsonArray schemaFields = schemaObject.getAsJsonArray("fields");
//        JsonArray valuesObject = valueObject.getAsJsonArray("values");
//
//        logger.info("schemaFields");
//        System.out.println(schemaFields);
//
//
//
//        Map<JsonElement, JsonElement> map = new HashMap<>();
//        for(JsonElement field:  schemaFields){
//          JsonObject jsonObjectField = (JsonObject) field;
//          JsonElement fieldName = jsonObjectField.get("name");
//          JsonElement fieldIndex =jsonObjectField.get("index");
//          map.put(fieldName, fieldIndex);
//        }
//
//        logger.info("valuesObject");
//        System.out.println(valuesObject);
//
//        JsonArray tableColumnsMetaData= null;
//        JsonArray afterValues = null;
//        JsonElement operationType =valuesObject.get(3);
//
//        if(!operationType.getAsString().equalsIgnoreCase("d")){
//          System.out.println("yesss");
//          JsonObject  after = (JsonObject) valuesObject.get(1);
//          JsonObject  afterSchema  = after.getAsJsonObject("schema");
//          afterValues = after.getAsJsonArray("values");
//          tableColumnsMetaData = afterSchema.getAsJsonArray("fields");
//
//        }
//
//        for (JsonElement fieldMetadata :tableColumnsMetaData ){
//          JsonObject dataType  = fieldMetadata.getAsJsonObject().get("schema").getAsJsonObject();
//          System.out.println(dataType);
//          System.out.println("+++");
//        }
//
//
//        System.out.println("__________________________________________________");
//        return  afterValues;

      } catch (Exception e) {
        logger.info(e.toString());
      }
    }
    return null;
  }



}
