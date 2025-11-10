package org.libs.core;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.libs.config.AppConfig;

public interface IDataFactory {
    SparkSession getSparkSession(AppConfig appConfig, SparkConf initialConf, String catalogName, String warehousePath);
}
