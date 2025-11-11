package org.libs.utils;

import org.libs.config.AppConfig;
import org.libs.config.properties.SparkProperties;

public class AppUtils {

    public static String toSnakeCase(String inputString) {
        if (inputString == null || inputString.isEmpty()) {
            return "";
        }

        String processedString = inputString;

        int lastDotIndex = processedString.lastIndexOf('.');
        if (lastDotIndex > 0) {
            processedString = processedString.substring(0, lastDotIndex);
        }

        String snakeCase = processedString
                .replaceAll("[-]+", "_")
                .replaceAll("([A-Z]+)", "_$1")
                .toLowerCase();

        if (snakeCase.startsWith("_")) {
            snakeCase = snakeCase.substring(1);
        }

        return snakeCase;
    }

    private static class IcebergConfigParams {
        final boolean useGlue;
        final String catalogName;
        final String defaultDatabase;

        public IcebergConfigParams(SparkProperties sparkProp) {
            this.useGlue = "glue".equalsIgnoreCase(sparkProp.getIceberg().getCatalogTypeValue());
            this.catalogName = sparkProp.getIceberg().getDefaultCatalogName();
            this.defaultDatabase = sparkProp.getIceberg().getDefaultGlueDatabase();
        }
    }

    public static String getFullTableName(AppConfig appConf, String tableName) {
        SparkProperties sparkProp = appConf.getSparkProp();
        IcebergConfigParams params = new IcebergConfigParams(sparkProp);

        String defaultTable = sparkProp.getIceberg().getDefaultTableName();
        String finalTableName = (tableName == null || tableName.isEmpty()) ? defaultTable : tableName;
        String standardizedTableName = AppUtils.toSnakeCase(finalTableName);

        if (params.useGlue) {
            return String.join(".",
                    params.catalogName,
                    params.defaultDatabase,
                    standardizedTableName);
        } else {
            return params.catalogName + "." + standardizedTableName;
        }
    }
}