package org.libs.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.libs.core.IDataFactory;
import org.libs.service.SparkService;
import org.springframework.stereotype.Component;

@Component
public class DataFactory implements IDataFactory {

    @Override
    public SparkSession getSparkSession(AppConfig appConfig, SparkConf initialConf, String catalogName, String warehousePath) {
        return SparkService.getOrCreateSparkSession(appConfig, initialConf, catalogName, warehousePath);
    }
}
