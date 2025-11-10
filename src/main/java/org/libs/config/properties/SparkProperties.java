package org.libs.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Collectors;

@Component
@ConfigurationProperties(prefix = "spark")
@Data
public class SparkProperties {
    private String appName;
    private String master;

    @Override
    public String toString() {
        Field[] fields = this.getClass().getDeclaredFields();

        String propertiesString = Arrays.stream(fields)
                .map(field -> {
                    try {
                        field.setAccessible(true);
                        String name = field.getName();
                        Object value = field.get(this);
                        return name + " = [" + value + "]";
                    } catch (IllegalAccessException e) {
                        return field.getName() + " = [Error reading value: " + e.getMessage() + "]";
                    }
                })
                .collect(Collectors.joining(",\n    ", "SparkProperties {\n    ", "\n}"));

        return propertiesString;
    }

    private S3Values s3 = new S3Values();
    private IcebergValues iceberg = new IcebergValues();
    private AthenaValues athena = new AthenaValues();

    @Data
    public static class S3Values {
        private String bucketName;
        private String folderPath;

        @Override
        public String toString() {
            Field[] fields = this.getClass().getDeclaredFields();
            return Arrays.stream(fields)
                    .map(field -> {
                        try {
                            field.setAccessible(true);
                            return field.getName() + "=" + field.get(this);
                        } catch (IllegalAccessException e) {
                            return field.getName() + "=ERROR";
                        }
                    })
                    .collect(Collectors.joining(", ", "S3Values {", "}"));
        }

        public String getWarehouseRoot() {
            return "s3a://" + bucketName + "/" + folderPath;
        }
    }

    @Data
    public static class IcebergValues {
        private String defaultCatalogName;
        private String defaultTableName;
        private String catalogTypeValue;
        private String defaultGlueDatabase;

        @Override
        public String toString() {
            Field[] fields = this.getClass().getDeclaredFields();
            return Arrays.stream(fields)
                    .map(field -> {
                        try {
                            field.setAccessible(true);
                            return field.getName() + "=" + field.get(this);
                        } catch (IllegalAccessException e) {
                            return field.getName() + "=ERROR";
                        }
                    })
                    .collect(Collectors.joining(", ", "IcebergValues {", "}"));
        }

    }

    @Data
    public static class AthenaValues {

        private String workgroup;
        private String s3OutputLocation;
        private String database;
        private String driver;
        private String jdbcUrl;

        @Override
        public String toString() {
            Field[] fields = this.getClass().getDeclaredFields();
            return Arrays.stream(fields)
                    .map(field -> {
                        try {
                            field.setAccessible(true);
                            return field.getName() + "=" + field.get(this);
                        } catch (IllegalAccessException e) {
                            return field.getName() + "=ERROR";
                        }
                    })
                    .collect(Collectors.joining(", ", "AthenaValues {", "}"));
        }

    }

}