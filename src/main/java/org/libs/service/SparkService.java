package org.libs.service;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.libs.config.AppConfig;
import org.libs.config.properties.S3Properties;
import org.libs.config.properties.SparkProperties;
import org.apache.log4j.Logger;
import org.libs.utils.LogUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.libs.contanst.AppCont.SPARK_SQL_SESSION_TIME_ZONE;
import static org.libs.contanst.SparkCont.*;

public class SparkService {

    private static final Logger logger = Logger.getLogger(SparkService.class);

    public static SparkConf confForParquet(AppConfig inputConfig) {
        logger.debug("Bắt đầu cấu hình SparkConf cơ bản cho Parquet/S3.");

        SparkProperties sparkProp = inputConfig.getSparkProp();
        S3Properties s3Prop = inputConfig.getS3Properties();

        SparkConf conf = new SparkConf().setAppName(sparkProp.getAppName())
                .setMaster(sparkProp.getMaster());

        conf.set(SPARK_HADOOP_FS_S3A_ENDPOINT, s3Prop.getEndpoint());
        conf.set(SPARK_HADOOP_FS_S3A_ACCESS_KEY, s3Prop.getAccessKey());
        conf.set(SPARK_HADOOP_FS_S3A_SECRET_KEY, s3Prop.getSecretKey());

        String provider = inputConfig.getS3Properties().isStaticKeys()
                ? ORG_APACHE_HADOOP_FS_S3A_SIMPLE_AWS_CREDENTIALS_PROVIDER
                : COM_AMAZONAWS_AUTH_PROFILE_PROFILE_CREDENTIALS_PROVIDER;

        conf.set(SPARK_HADOOP_FS_S3A_AWS_CREDENTIALS_PROVIDER, provider);
        conf.set(SPARK_HADOOP_FS_S3A_PATH_STYLE_ACCESS, "true");
        conf.set(SPARK_HADOOP_FS_S3A_IMPL, ORG_APACHE_HADOOP_FS_S3A_S3AFILESYSTEM);
        conf.set(SPARK_HADOOP_FS_S3A_CONNECTION_SSL_ENABLED, "false");

        conf.set(SPARK_UI_ENABLED, "false");
        conf.set(SPARK_UI_SHOW_CONSOLE_PROGRESS, "false");

        logger.debug("Đã hoàn thành cấu hình SparkConf cơ bản.");
        return conf.clone();
    }

    public static SparkConf confForIceberg(AppConfig config, String catalogName, String warehousePath) {
        SparkProperties sparkProp = config.getSparkProp();
        String typeValue = sparkProp.getIceberg().getCatalogTypeValue();
        boolean useGlue = "glue".equalsIgnoreCase(typeValue);

        String warehouse = (warehousePath == null || warehousePath.isEmpty())
                ? sparkProp.getS3().getWarehouseRoot()
                : warehousePath;
        String finalCatalog = (catalogName == null || catalogName.isEmpty())
                ? sparkProp.getIceberg().getDefaultCatalogName()
                : catalogName;

        SparkConf conf = confForParquet(config);

        String prefix = SPARK_SQL_CATALOG + "." + finalCatalog;
        conf.set(SPARK_SQL_CATALOG + "." + finalCatalog, ORG_APACHE_ICEBERG_SPARK_SPARKCATALOG);
        conf.set(SPARK_SQL_EXTENSIONS, ORG_APACHE_ICEBERG_SPARK_EXTENSIONS_ICEBERGSPARKSESSIONEXTENSIONS);

        if (useGlue) {
            conf.set(prefix + ".catalog-impl", ORG_APACHE_ICEBERG_AWS_GLUE_GLUECATALOG);
            conf.set(prefix + ".io-impl", ORG_APACHE_ICEBERG_AWS_S3_S3FILEIO);
            conf.set(prefix + ".warehouse", warehouse);
            conf.set(prefix + ".region", config.getS3Properties().getRegion());
            conf.set(prefix + ".default-glue-database",
                    sparkProp.getIceberg().getDefaultGlueDatabase());
        } else {
            conf.set(prefix + ".catalog-impl", ORG_APACHE_ICEBERG_SPARK_SPARKCATALOG);
            conf.set(prefix + ".type", "hadoop");
            conf.set(prefix + ".warehouse", warehouse);
        }

        conf.set(prefix + ".spark.write.schema.evolution-enabled", "true");
        conf.set(
                prefix + ".default-table-properties",
                "write.metadata.delete-enabled=true, write.delete.mode=merge-on-read"
        );
        conf.set(SPARK_SQL_SESSION_TIME_ZONE, "UTC");
        conf.set("spark.sql.legacy.allowNonExistentSchemaEvolution", "true");

        return conf;
    }

    public static SparkSession getOrCreateSparkSession(
            AppConfig appConfig,
            SparkConf initialConf,
            String catalogName,
            String warehousePath) {

        SparkConf finalConf = (initialConf == null) ? confForIceberg(appConfig, catalogName, warehousePath) : initialConf.clone();

        SparkSession spark = SparkSession.builder().config(finalConf).getOrCreate();

        logger.info(String.format("SparkSession sẵn sàng. Catalog [%s] (%s)", catalogName, warehousePath));

        return spark;
    }

    public static void writeToIcebergTable(SparkSession spark, Dataset<Row> df, String fullTableName) {

        logger.info("Bắt đầu ghi DataFrame vào bảng Iceberg: " + fullTableName);

        df.write()
                .format("iceberg")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(fullTableName);

        logger.info("Đã ghi thành công dữ liệu vào bảng Iceberg: " + fullTableName);
    }

    public static void upsertToIcebergTable(SparkSession spark, Dataset<Row> df, String fullTableName, List<String> primaryKeys) throws IOException {

        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("Cần ít nhất 1 primary key để thực hiện UPSERT.");
        }

        String batchViewName = "incoming_batch_" + System.currentTimeMillis();
        df.createOrReplaceTempView(batchViewName);

        String onCondition = primaryKeys.stream()
                .map(key -> String.format("target.%s = source.%s", key, key))
                .collect(Collectors.joining(" AND "));

        String mergeSql = String.format("""
                MERGE INTO %s AS target
                USING %s AS source
                ON %s
                WHEN MATCHED THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
                """, fullTableName, batchViewName, onCondition);

        logger.info("Bắt đầu thao tác MERGE INTO (Upsert) với điều kiện ON: " + onCondition);
        spark.sql(mergeSql);

        logger.info("Hoàn tất UPSERT nguyên tử cho bảng " + fullTableName);
        spark.catalog().dropTempView(batchViewName);
    }

    public static Dataset<Row> readJsonListToDataFrame(SparkSession spark, List<String> jsonList) {

        logger.info("Bắt đầu đọc danh sách JSON (" + jsonList.size() + " dòng) để tạo DataFrame...");

        Dataset<String> jsonDS = spark.createDataset(jsonList, Encoders.STRING());
        return spark.read()
                .option("multiline", "true")
                .option("inferSchema", "true")
                .option("mode", "PERMISSIVE")
                .json(jsonDS);
    }

    public static boolean doesTableExist(SparkSession spark, String fullTableName) {
        logger.info("Bắt đầu kiểm tra sự tồn tại của bảng trong Catalog: '" + fullTableName + "'...");

        try {
            boolean exists = spark.catalog().tableExists(fullTableName);

            if (exists) {
                logger.info("Kiểm tra Catalog: Bảng '" + fullTableName + "' TỒN TẠI.");
            } else {
                logger.info("Kiểm tra Catalog: Bảng '" + fullTableName + "' KHÔNG TỒN TẠI.");
            }

            return exists;
        } catch (Exception e) {
            logger.error("LỖI khi kiểm tra sự tồn tại của bảng '" + fullTableName + "'.", e);

            return false;
        }
    }

    public static Dataset<Row> readAthenaData(SparkSession sparkSession, AppConfig appConfig, String sqlQuery) {
        logger.info(String.format("Bắt đầu truy vấn Athena: [%s]", sqlQuery));

        if (sparkSession == null || appConfig == null || sqlQuery == null || sqlQuery.isEmpty()) {
            logger.error("SparkSession, AppConfig hoặc SQL Query không hợp lệ.");
            throw new IllegalArgumentException("Tham số đầu vào cho readAthenaData không hợp lệ.");
        }

        SparkProperties sparkProp = appConfig.getSparkProp();

        String jdbcUrl = sparkProp.getAthena().getJdbcUrl();
        String workgroup = sparkProp.getAthena().getWorkgroup();
        String s3OutputLocation = sparkProp.getAthena().getS3OutputLocation();
        String database = sparkProp.getAthena().getDatabase();

        if (jdbcUrl == null || s3OutputLocation == null) {
            logger.fatal("Cấu hình Athena JDBC URL hoặc S3 Output Location bị thiếu trong AppConfig.");
            throw new RuntimeException("Cấu hình Athena không đầy đủ.");
        }

        Properties connectionProperties = new Properties();

        connectionProperties.put(ATHENA_WORKGROUP, workgroup);
        connectionProperties.put(ATHENA_S3_OUTPUT_LOCATION, s3OutputLocation);

        try {

            Dataset<Row> result = sparkSession.read().format(JDBC_FORMAT)
                    .option(JDBC_DRIVER, sparkProp.getAthena().getDriver())
                    .option(JDBC_URL, jdbcUrl)
                    .option("dbtable", "(" + sqlQuery + ") AS custom_query")
                    .option("database", database).jdbc(jdbcUrl, "(" + sqlQuery + ") AS custom_query", connectionProperties);

            logger.info("Truy vấn Athena thành công. Trả về Dataset.");
            return result;

        } catch (Exception e) {
            LogUtils.logProcessingError(logger, e, String.format("Lỗi khi truy vấn Athena với SQL: [%s]", sqlQuery));
            throw new RuntimeException("Lỗi trong quá trình truy vấn dữ liệu từ Athena.", e);
        }
    }

}