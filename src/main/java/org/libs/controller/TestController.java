package org.libs.controller;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.libs.config.AppConfig;
import org.libs.core.IDataFactory;
import org.libs.model.DataInfo;
import org.libs.service.FlattenRecursivelyDb;
import org.libs.service.RecursiveFlattener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.libs.contanst.AppCont.GLUE_CATALOG;

@RestController
@RequestMapping("/api/v1/")
public class TestController {

    @Autowired
    private IDataFactory dataFactory;

    @Autowired
    FlattenRecursivelyDb fRecurDb;

    @Autowired
    AppConfig appConf;

    @GetMapping("/testFlatten")
    public ResponseEntity<String> testFlatten() throws Exception {
        SparkSession spark;
        spark = dataFactory.getSparkSession(appConf, null, GLUE_CATALOG, null);

        Dataset<Row> df = spark.read()
                .option("multiLine", true)
                .json("sample.json");
        DataInfo dataInfo = new DataInfo();
        dataInfo.setIdField("pzInskey");
        dataInfo.setDateField("pxCreateDateTime");
        dataInfo.setDateFormat("yyyyMMdd'T'HHmmss.SSS 'GMT'");
        dataInfo.setFilePath("999999");
        dataInfo.setUseRecursive(true);

        RecursiveFlattener.processDataFrame(df, spark, dataInfo, "glue_catalog.aws_database.pega_main");

        spark.stop();
        return ResponseEntity
                .status(HttpStatus.OK)
                .body("OK");
    }
}
