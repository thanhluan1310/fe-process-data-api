package org.libs.core;

import org.apache.spark.sql.SparkSession;
import org.libs.model.DataInfo;

import java.util.List;

public interface IDatabase {

    void setDataInfo(DataInfo dataInfo);

    void writeFullBatch(SparkSession spark, List<String> batch, String tableName) throws Exception;

    void write(SparkSession spark, String jsonData, String tableName) throws Exception;
}