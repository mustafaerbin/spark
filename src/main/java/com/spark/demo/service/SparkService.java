package com.spark.demo.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface SparkService {

    Dataset<Row> getDataSet(final String path);
}
