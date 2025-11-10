package org.libs.utils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import java.util.*;

import static org.apache.spark.sql.functions.*;

public class HashUtil {

    private static Column safeToString(Column column, DataType dataType) {
        if (dataType instanceof ArrayType || dataType instanceof StructType || dataType instanceof MapType) {
            return to_json(column);
        }
        return column.cast("string");
    }

    public static Dataset<Row> createHashKey(Dataset<Row> df, List<Column> hashCols, String hashColumnName) {
        List<Column> safeCols = new ArrayList<>();
        for (Column col : hashCols) {
            String colName = col.toString();
            DataType dt = df.schema().apply(colName).dataType();
            safeCols.add(safeToString(col, dt));
        }
        Column joined = concat_ws("||", safeCols.toArray(new Column[0]));
        return df.withColumn(hashColumnName, sha2(joined, 256));
    }
}
