package com.spark.demo.service.impl;

import com.spark.demo.service.SparkService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class SparkServiceImpl implements SparkService {

    private final SparkSession sparkSession;

    @Autowired
    public SparkServiceImpl(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public Dataset<Row> getDataSet(final String path) {
        return sparkSession.read().format("csv")
                .option("header", "true")
                .load(path);
    }
}
