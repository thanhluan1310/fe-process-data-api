package org.libs.config;

import lombok.Data;
import org.apache.log4j.Logger;
import org.libs.config.properties.AppProperties;
import org.libs.config.properties.S3Properties;
import org.libs.config.properties.SparkProperties;
import org.springframework.stereotype.Component;

import static org.libs.contanst.S3Cont.S3_ENDPOINT;
import static org.libs.contanst.S3Cont.S3_ENDPOINT_DOWNLOAD;

@Component
@Data
public class AppConfig {
    private static AppConfig INSTANCE;

    private static final Logger logger = Logger.getLogger(AppConfig.class);

    private final String s3Endpoint;

    private final String s3EndpointDownload;

    private S3Properties s3Properties;

    private AppProperties appProp;

    private SparkProperties sparkProp;

    public AppConfig(AppProperties appProp, S3Properties s3Properties, SparkProperties sparkProp) {
        logger.info("Bắt đầu tải cấu hình ứng dụng từ System Properties.");

        this.appProp = appProp;
        this.s3Properties = s3Properties;
        this.sparkProp = sparkProp;

        this.s3Endpoint = getProperty(S3_ENDPOINT, s3Properties.getEndpoint());
        this.s3EndpointDownload = getProperty(S3_ENDPOINT_DOWNLOAD, s3Properties.getEndpointDownload());

        logger.info("Hoàn thành tải cấu hình.");
    }

    private String getProperty(String key, String defaultValue) {
        String value = System.getProperty(key);
        if (value != null && !value.isEmpty()) {
            logger.debug(String.format("Đã ghi đè [%s] bằng System Property: %s", key, value));
            return value;
        }
        return defaultValue;
    }

    public static AppConfig getInstance() {
        AppProperties appProp = new AppProperties();
        S3Properties s3Properties = new S3Properties();
        SparkProperties sparkProp = new SparkProperties();
        return new AppConfig(appProp, s3Properties, sparkProp);
    }
}