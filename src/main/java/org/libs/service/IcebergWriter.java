package org.libs.service;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.libs.model.DataInfo;
import org.libs.utils.SchemaUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.libs.contanst.AppCont.FILE_KEY;
import static org.libs.contanst.SparkCont.*;

public class IcebergWriter {
    private static final Logger logger = LogManager.getLogger(IcebergWriter.class);

    public static void save(Dataset<Row> df, SparkSession spark, String tableName,
                            DataInfo dataInfo, List<String> keys, boolean hasDatePartition)
            throws TableAlreadyExistsException, IOException {

//        if(!tableName.equals("glue_catalog.aws_database.pega_main")){
//            return;
//        }
        boolean tableExists = SparkService.doesTableExist(spark, tableName);

        df = df.withColumn(FILE_KEY, lit(dataInfo.getFilePath()));

        if (hasDatePartition) {
            df = df.withColumn(dataInfo.getDateField(),
                    to_timestamp(col(dataInfo.getDateField()), dataInfo.getDateFormat()));
        }

        if (!tableExists) {
            logger.info("Creating new Iceberg table: " + tableName);

            List<Column> partitions = new ArrayList<>();
            partitions.add(bucket(16, col(dataInfo.getIdField()))); // bucket ID
            if (hasDatePartition) {
                partitions.add(col(dataInfo.getDateField())); // date partition
            }

            Column firstPartition = partitions.get(0);
            Column[] restPartitions = partitions.size() > 1
                    ? partitions.subList(1, partitions.size()).toArray(new Column[0])
                    : new Column[0];

            df.writeTo(tableName)
                    .partitionedBy(firstPartition, restPartitions)
                    .tableProperty(WRITE_METADATA_DELETE_ENABLED, "true")
                    .tableProperty(WRITE_DELETE_MODE, MERGE_ON_READ)
                    .tableProperty(WRITE_MERGE_MODE, COPY_ON_WRITE)
                    .create();

            logger.info("Iceberg table " + tableName + " created with partitioning");

        } else {
            logger.info("Table exists, handling schema evolution if needed...");
            Dataset<Row> existing = spark.table(tableName);
            SchemaUtils.evolveTableSchema(spark, tableName, existing.schema(), df.schema());
            Dataset<Row> alignedDf = SchemaUtils.alignSchema(df, existing.schema());
            SparkService.upsertToIcebergTable(spark, alignedDf, tableName, keys);
        }
    }

}
