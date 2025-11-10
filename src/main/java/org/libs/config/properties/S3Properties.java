package org.libs.config.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Collectors;

@Component
@ConfigurationProperties(prefix = "s3")
@Data
public class S3Properties {

    private String endpointDownload;
    private String accessKeyDownload;
    private String secretKeyDownload;
    private String bucketNameDownload;

    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String region;
    private boolean staticKeys;

    public boolean isStaticKeys() {
        return staticKeys;
    }

    @Override
    public String toString() {
        Field[] fields = this.getClass().getDeclaredFields();

        return Arrays.stream(fields)
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
                .collect(Collectors.joining(",\n    ", "S3Properties {\n    ", "\n}"));
    }
}