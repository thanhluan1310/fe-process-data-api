package org.libs.core.service.schema;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public interface IDataFrameAligner {

    Dataset<Row> align(SparkSession spark, Dataset<Row> incomingDf, String fullTableName) throws Exception;

}
