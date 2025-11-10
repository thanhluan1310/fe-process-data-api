package org.libs.controller;

import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.libs.config.AppConfig;
import org.libs.core.IDataFactory;
import org.libs.model.DataInfo;
import org.libs.service.FlattenRecursivelyDb;
import org.libs.utils.AppUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.libs.contanst.AppCont.GLUE_CATALOG;

@RestController
@RequestMapping("/api/v1/")
public class DemoController {
    private static final Logger logger = Logger.getLogger(DemoController.class);

    @Value("${spark.use.recursive}")
    private boolean useRecursive;

    @Autowired
    private IDataFactory dataFactory;

    @Autowired
    FlattenRecursivelyDb fRecurDb;

    @Autowired
    AppConfig appConf;

    @Autowired
    DataInfo dataInfo;

    @GetMapping("/Demo")
    public ResponseEntity<String> Demo() throws Exception {
        String jsonData = readFileToString("data.json");
        dataInfo.setIdField("pzInskey");
        dataInfo.setDateField("pxCreateDateTime");
        dataInfo.setDateFormat("yyyyMMdd'T'HHmmss.SSS 'GMT'");
        dataInfo.setUseRecursive(useRecursive);

        SparkSession spark = dataFactory.getSparkSession(appConf, null, GLUE_CATALOG, null);
        fRecurDb.setDataInfo(dataInfo);

        String tableName = AppUtils.getFullTableName(appConf, "pega_main");
        fRecurDb.write(spark, jsonData, tableName);

        return ResponseEntity
                .status(HttpStatus.OK)
                .body("OK");
    }

    private static String readFileToString(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        return Files.readString(path, StandardCharsets.UTF_8);
    }
}
