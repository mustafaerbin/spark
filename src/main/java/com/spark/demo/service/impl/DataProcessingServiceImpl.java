package com.spark.demo.service.impl;

import com.spark.demo.service.DataProcessingService;
import com.spark.demo.service.MinioService;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.spark.sql.functions.*;

@Service
@Transactional
public class DataProcessingServiceImpl implements DataProcessingService {

    private final MinioClient minioClient;
    private final SparkSession sparkSession;
    @Autowired
    MinioService minioService;

    @Autowired
    public DataProcessingServiceImpl(MinioClient minioClient, SparkSession sparkSession) {
        this.minioClient = minioClient;
        this.sparkSession = sparkSession;
    }

    public void processAndUploadData() throws RuntimeException, IOException {
        // MinIO'dan CSV dosyalarını yükleme
        String personPath = "s3a://demographics/person_data.csv";
        String countryPath = "s3a://demographics/country_data.csv";

        Dataset<Row> personDF = sparkSession.read().option("header", "true").csv(personPath);
        Dataset<Row> countryDF = sparkSession.read().option("header", "true").csv(countryPath);

        // Veri işleme
        Dataset<Row> filteredDF = personDF.filter(col("age").gt(30)
                .and(col("blood_type").isin("A+", "A-", "AB+", "AB-")));

        Dataset<Row> resultDF = filteredDF.join(countryDF, "country_code")
                .groupBy("country_name")
                .agg(
                        collect_list("first_name").alias("names"),
                        count("first_name").alias("count")
                )
                .withColumn("name_string", concat_ws(", ", col("names")))
                .select("country_name", "count", "name_string");

        // Sonuçları CSV'ye yazma
        String resultPath = "s3a://demographics/output/result.csv";
        resultDF.write().option("header", "true").csv(resultPath);
    }

    public void processData() throws Exception {

        // Download CSV files from MinIO
        String personDataPath = "C:/minio_data/person_data.csv";
        String countryDataPath = "C:/minio_data/country_data.csv";

        minioService.downloadFileLocale("person_data.csv", personDataPath);
        minioService.downloadFileLocale("country_data.csv", countryDataPath);

        // Load the CSV files into DataFrames
        Dataset<Row> personDF = sparkSession.read().format("csv")
                .option("header", "true")
                .load(personDataPath);

        Dataset<Row> countryDF = sparkSession.read().format("csv")
                .option("header", "true")
                .load(countryDataPath);

        // Filtreleme ve birleştirme işlemleri
        Dataset<Row> filtered = personDF.filter(
                "year(current_date()) - year(birthday) > 30 AND blood_type IN ('A+', 'A-', 'AB+', 'AB-')");


        Dataset<Row> joined = filtered.join(countryDF, "country");

        Dataset<Row> result = joined.groupBy("country_name")
                .agg(
                        count("country").alias("count"),
                        functions.concat_ws(", ", functions.collect_list("first_name")).alias("names") // names sütununu düz metin olarak birleştir
                );

        // Veriyi CSV formatında kaydetme
        result.coalesce(1) // Tek dosya yapmak için
                .write()
                .option("header", "true") // Başlık satırını dahil et
                .csv("C:/minio_data/output.csv");

        // Upload result to MinIO
        minioClient.uploadObject(
                io.minio.UploadObjectArgs.builder()
                        .bucket("results")
                        .object("output.csv")
                        .filename("C:/minio_data/output.csv")
                        .build()
        );
    }
}