package org.libs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.*;
import org.libs.service.IcebergWriter;
import org.libs.utils.HashUtil;

import static org.apache.spark.sql.functions.*;
import static org.libs.contanst.AppCont.HASH_KEY_COL;
import static org.libs.contanst.AppCont.PARENT_ID_COL;
import static org.libs.contanst.SparkCont.SPARK_UI_ENABLED;
import static org.libs.contanst.SparkCont.SPARK_UI_SHOW_CONSOLE_PROGRESS;

import java.util.*;
import java.util.stream.Stream;

public class RecursiveFlattenDebug {

    static SparkSession spark;

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.broadcast").setLevel(Level.WARN);
        Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN);

        spark = SparkSession.builder()
                .appName("RecursiveFlattenDebug")
                .master("local[*]")
                .config(SPARK_UI_ENABLED, "false")
                .config(SPARK_UI_SHOW_CONSOLE_PROGRESS, "false")
                .config("spark.ui.showConsoleProgress", "false")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("multiLine", true)
                .json("sample.json");

        df.printSchema();
        df.show(false);
        spark.range(10).show();

        flattenRecursively(df, "root", spark, "root", 0);

        spark.stop();

    }

    /**
     * Hàm flatten đệ quy — tương tự kiểu mày đang dùng
     */
    static void flattenRecursively(Dataset<Row> df, String parentKey, SparkSession spark, String tableName, int level) {
        String indent = "  ".repeat(level);
        System.out.println(indent + "👉 Level " + level + " flattening parent: " + parentKey);
        df.printSchema();

        StructType schema = df.schema();

        // Bỏ cột quản lý
        List<StructField> fieldsToProcess = Arrays.stream(schema.fields())
                .filter(f -> !f.name().equals(PARENT_ID_COL) && !f.name().equals(HASH_KEY_COL))
                .toList();

        //  Nếu root là array
        if (fieldsToProcess.size() == 1 && fieldsToProcess.get(0).dataType() instanceof ArrayType) {
            String rootArrayCol = fieldsToProcess.get(0).name();
            Dataset<Row> explodedRoot = df
                    .withColumn(PARENT_ID_COL, monotonically_increasing_id())
                    .withColumn("element", explode_outer(col(rootArrayCol)))
                    .select(col(PARENT_ID_COL), col("element.*"));
            flattenRecursively(explodedRoot, PARENT_ID_COL, spark, tableName, level++);
            return;
        }

        for (StructField field : fieldsToProcess) {
            String colName = field.name();
            DataType type = field.dataType();

            if (type instanceof StringType) {
                try {
                    int SAMPLE_LIMIT = 5;
                    List<String> sampleJsons = df.filter(col(colName).isNotNull())
                            .select(col(colName))
                            .as(Encoders.STRING())
                            .takeAsList(SAMPLE_LIMIT);

                    if (sampleJsons != null && !sampleJsons.isEmpty()) {
                        List<String> candidates = new ArrayList<>();
                        for (String s : sampleJsons) {
                            if (s == null) continue;
                            String t = s.trim();
                            if ((t.startsWith("{") && t.contains(":")) || (t.startsWith("[") && t.contains(":"))) {
                                candidates.add(t);
                            }
                        }

                        if (!candidates.isEmpty()) {
                            StructType structSchema = null;
                            ArrayType arraySchema = null;

                            boolean isArray = candidates.get(0).trim().startsWith("[");
                            try {
                                Dataset<String> sampleDs = spark.createDataset(candidates, Encoders.STRING());
                                StructType tmpSchema = spark.read().json(sampleDs).schema();

                                if (isArray) {
                                    arraySchema = DataTypes.createArrayType(tmpSchema, true);
                                    df = df.withColumn(colName, from_json(col(colName), arraySchema));
                                    type = df.schema().apply(colName).dataType();
                                } else {
                                    structSchema = tmpSchema;
                                    df = df.withColumn(colName, from_json(col(colName), structSchema));
                                    type = df.schema().apply(colName).dataType();
                                }

                                schema = df.schema();

                            } catch (Exception e) {
                            }
                        }
                    }
                } catch (Exception ignored) {
                }
            }

            // === ARRAY TYPE ===

            if (type instanceof ArrayType arrType) {
                String childTable = tableName + "_" + colName.toLowerCase();
                List<String> childKeys = List.of(PARENT_ID_COL, HASH_KEY_COL);

                Dataset<Row> nonEmptyDf = df.filter(col(colName).isNotNull().and(size(col(colName)).gt(0)));
                if (nonEmptyDf.isEmpty()) continue;

                Dataset<Row> exploded = nonEmptyDf
                        .withColumn(PARENT_ID_COL, lit(parentKey))
                        .select(col(PARENT_ID_COL), explode_outer(col(colName)).alias(colName))
                        .filter(col(colName).isNotNull());

                if (arrType.elementType() instanceof StructType structType) {
                    Dataset<Row> childDf = exploded.select(col(PARENT_ID_COL), col(colName + ".*"));

                    List<String> scalarFields = Arrays.stream(structType.fields())
                            .filter(f -> !(f.dataType() instanceof ArrayType) && !(f.dataType() instanceof StructType))
                            .map(StructField::name)
                            .toList();

                    Dataset<Row> childDfToSave;
                    List<Column> hashCols = new ArrayList<>();
                    hashCols.add(col(PARENT_ID_COL));

                    boolean needsRowIndex = scalarFields.isEmpty();
                    if (!needsRowIndex) {
                        String[] selectExprs = Stream.concat(Stream.of(PARENT_ID_COL), scalarFields.stream()).toArray(String[]::new);
                        childDfToSave = childDf.selectExpr(selectExprs);
                        scalarFields.stream().map(functions::col).forEach(hashCols::add);
                    } else {
                        WindowSpec w = org.apache.spark.sql.expressions.Window.partitionBy(col(PARENT_ID_COL)).orderBy(lit(1));
                        childDfToSave = childDf
                                .withColumn("_row_idx", row_number().over(w))
                                .select(col(PARENT_ID_COL), col("_row_idx")); // chỉ giữ parent_id + _row_idx
                        hashCols.add(col("_row_idx"));
                    }

                    childDfToSave = HashUtil.createHashKey(childDfToSave, hashCols, HASH_KEY_COL);
                    if (!needsRowIndex && childDfToSave.columns().length > scalarFields.size() + 2)
                        childDfToSave = childDfToSave.drop("_row_idx");

                    childDfToSave = childDfToSave.dropDuplicates(childKeys.toArray(new String[0]));
                    childDfToSave.show();
                    System.out.println("child table " + childTable);

                    flattenRecursively(childDf, PARENT_ID_COL, spark, childTable, level++);
                } else {
                    Dataset<Row> arrDf = exploded
                            .select(col(PARENT_ID_COL), col(colName).alias("value"))
                            .filter(col("value").isNotNull());
                    arrDf = HashUtil.createHashKey(arrDf, List.of(col(PARENT_ID_COL), col("value")), HASH_KEY_COL);
                    arrDf = arrDf.dropDuplicates(childKeys.toArray(new String[0]));
                    arrDf.show(false);
                    System.out.println("child table " + childTable);
                }
            }

            // === STRUCT TYPE ===
            else if (type instanceof StructType structType) {
                String childTable = tableName + "_" + colName.toLowerCase();
                List<String> childKeys = List.of(PARENT_ID_COL, HASH_KEY_COL);

                Dataset<Row> childDf = df
                        .withColumn(PARENT_ID_COL, lit(parentKey))
                        .select(col(PARENT_ID_COL), col(colName + ".*"))
                        .filter(col(colName).isNotNull());

                if (childDf.isEmpty()) continue;

                List<String> scalarFields = Arrays.stream(structType.fields())
                        .filter(f -> !(f.dataType() instanceof ArrayType) && !(f.dataType() instanceof StructType))
                        .map(StructField::name)
                        .toList();

                Dataset<Row> childDfToSave;
                List<Column> hashCols = new ArrayList<>();
                hashCols.add(col(PARENT_ID_COL));

                boolean needsRowIndex = scalarFields.isEmpty();
                if (!needsRowIndex) {
                    String[] selectExprs = Stream.concat(Stream.of(PARENT_ID_COL), scalarFields.stream()).toArray(String[]::new);
                    childDfToSave = childDf.selectExpr(selectExprs);
                    scalarFields.stream().map(functions::col).forEach(hashCols::add);
                } else {
                    WindowSpec w = org.apache.spark.sql.expressions.Window.partitionBy(col(PARENT_ID_COL)).orderBy(lit(1));
                    childDfToSave = childDf
                            .withColumn("_row_idx", row_number().over(w))
                            .select(col(PARENT_ID_COL), col("_row_idx")); // chỉ giữ parent_id + _row_idx
                    hashCols.add(col("_row_idx"));
                }

                childDfToSave = HashUtil.createHashKey(childDfToSave, hashCols, HASH_KEY_COL);
                if (!needsRowIndex && childDfToSave.columns().length > scalarFields.size() + 2)
                    childDfToSave = childDfToSave.drop("_row_idx");

                childDfToSave = childDfToSave.dropDuplicates(childKeys.toArray(new String[0]));

                childDfToSave.show(false);
                System.out.println("child table " + childTable);
                flattenRecursively(childDf, PARENT_ID_COL, spark, childTable, level++);
            }
        }
    }
}
