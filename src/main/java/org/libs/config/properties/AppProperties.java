package org.libs.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Collectors;

@Component
@ConfigurationProperties(prefix = "app")
@Data
public class AppProperties {

    private String caseId;
    private String pvStreamColumnName;
    private String pxUpdateDateTimeColumn;
    private String pxUpdateDateTimeTsColumn;

    private Integer pegaHexPrefixLength;
    private Integer batchSize;
    private char csvDelimiter;
    private String pegaDateFormat;
    private String yamlPathFile;

    private boolean hasHeader;
    private String clientType;

    public boolean getHasHeader() {
        return hasHeader;
    }

    @Override
    public String toString() {
        // Lấy tất cả các field được khai báo trong class này
        Field[] fields = this.getClass().getDeclaredFields();

        // Sử dụng Stream API để xây dựng chuỗi thuộc tính
        String propertiesString = Arrays.stream(fields)
                .map(field -> {
                    try {
                        // Cho phép truy cập vào các thuộc tính private
                        field.setAccessible(true);
                        String name = field.getName();
                        Object value = field.get(this);
                        return name + " = [" + value + "]";
                    } catch (IllegalAccessException e) {
                        return field.getName() + " = [Error reading value: " + e.getMessage() + "]";
                    }
                })
                .collect(Collectors.joining(",\n    ", "AppProperties {\n    ", "\n}"));

        return propertiesString;
    }
}
