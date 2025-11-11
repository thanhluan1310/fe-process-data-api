package org.libs;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.libs.service.RecursiveFlattener;
import org.libs.service.SparkService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {
    private static final SparkService svc = new SparkService();
    private static final Logger logger = Logger.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Application.class, args);
//        RecursiveFlattenDebug.main(args);
        logger.info("Application started successfully, SpringContextHolder initialized.");
    }
}