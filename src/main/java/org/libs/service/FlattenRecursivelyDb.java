package org.libs.service;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.libs.config.AppConfig;
import org.libs.config.properties.AppProperties;
import org.libs.config.properties.SparkProperties;
import org.libs.core.IDataFactory;
import org.libs.core.IDatabase;
import org.libs.model.DataInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.libs.contanst.AppCont.*;

@Service
public class FlattenRecursivelyDb implements IDatabase {
    private static final Logger logger = Logger.getLogger(FlattenRecursivelyDb.class);

    @Autowired
    private IDataFactory dataFactory;

    @Autowired
    private final AppConfig appConf;

    private DataInfo dataInfo;

    public FlattenRecursivelyDb(AppConfig appConf) {
        logger.debug("FlattenRecursivelyDb được khởi tạo qua Constructor Injection.");
        this.appConf = appConf;
    }

    @Override
    public void setDataInfo(DataInfo dataInfo) {
        this.dataInfo = dataInfo;
    }

    @Override
    public void writeFullBatch(SparkSession spark, List<String> batch, String tableName) throws Exception {
        AppProperties appProp = appConf.getAppProp();
        logger.info(String.format("Đã đạt kích thước batch (%d). Bắt đầu ghi batch ", appProp.getBatchSize()));

        if (batch.isEmpty()) {
            logger.error("Incoming DataFrame is null — aborting alignment.");
            throw new IllegalArgumentException("batch cannot be null.");
        }
        SparkProperties.IcebergValues iceberg = appConf.getSparkProp().getIceberg();

        String catalog = GLUE_CATALOG;
        String defaultTableName = iceberg.getDefaultTableName();
        String database = iceberg.getDefaultGlueDatabase();
        String catalogDb = catalog + "." + database;

        logger.info("Starting recursive flatten aligner...");
        logger.info("Catalog      : " + catalog);
        logger.info("Database     : " + database);
        logger.info("Catalog.DB   : " + catalogDb);
        logger.info("Default table name   : " + defaultTableName);
        logger.info("Full table   : " + tableName);

        Dataset<Row> df_Batch = SparkService.readJsonListToDataFrame(spark, batch);
        RecursiveFlattener.processDataFrame(
                df_Batch,
                spark,
                dataInfo,
                tableName
        );

        batch.clear();
    }

    @Override
    public void write(SparkSession spark, String jsonData, String tableName) throws Exception {
        List<String> batch = new ArrayList<>();
        batch.add(jsonData);
        writeFullBatch(spark, batch, tableName);
    }

}
