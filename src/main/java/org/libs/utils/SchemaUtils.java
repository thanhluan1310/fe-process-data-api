package org.libs.utils;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class SchemaUtils {

    public static void evolveTableSchema(SparkSession spark, String tableName,
                                         StructType existingSchema, StructType newSchema) {
        List<String> newCols = new ArrayList<>();
        for (StructField newField : newSchema.fields()) {
            boolean exists = Arrays.stream(existingSchema.fields())
                    .anyMatch(f -> f.name().equalsIgnoreCase(newField.name()));
            if (!exists) newCols.add(newField.name());
        }

        if (!newCols.isEmpty()) {
            String addSql = "ALTER TABLE " + tableName + " ADD COLUMNS (" +
                    newCols.stream()
                            .map(name -> {
                                StructField field = newSchema.fields()[newSchema.fieldIndex(name)];
                                return field.name() + " " + field.dataType().simpleString();
                            })
                            .collect(Collectors.joining(", ")) + ")";
            spark.sql(addSql);
        }
    }

    public static Dataset<Row> alignSchema(Dataset<Row> df, StructType existingSchema) {
        Dataset<Row> aligned = df;
        for (StructField f : existingSchema.fields()) {
            if (!Arrays.asList(df.columns()).contains(f.name())) {
                aligned = aligned.withColumn(f.name(), lit(null).cast(f.dataType()));
            }
        }
        return aligned;
    }
}
