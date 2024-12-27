package com.spark.demo.service.impl;

import com.spark.demo.service.DataProcessingService;
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

import java.io.IOException;
import java.io.InputStream;

import static org.apache.spark.sql.functions.*;

@Service
@Transactional
public class DataProcessingServiceImpl implements DataProcessingService {

    private final MinioClient minioClient;
    private final SparkSession sparkSession;

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
        // MinIO'dan CSV dosyalarını indir
        InputStream personData = minioClient.getObject(GetObjectArgs.builder()
                .bucket("data")
                .object("person_data.csv")
                .build());

        InputStream countryData = minioClient.getObject(GetObjectArgs.builder()
                .bucket("data")
                .object("country_data.csv")
                .build());

        // Apache Spark ile verileri yükle
        Dataset<Row> personDF = sparkSession.read().format("csv")
                .option("header", "true")
                .load(personData.toString());

        Dataset<Row> countryDF = sparkSession.read().format("csv")
                .option("header", "true")
                .load(countryData.toString());

        // Filtreleme ve birleştirme işlemleri
        Dataset<Row> filtered = personDF.filter(
                "age > 30 AND (blood_type IN ('A+', 'A-', 'AB+', 'AB-'))");

        Dataset<Row> joined = filtered.join(countryDF, "country_id");

        Dataset<Row> result = joined.groupBy("country_name")
                .agg(
                        count("id").alias("count"),
                        collect_list("first_name").alias("names")
                );

        // CSV olarak çıktı al ve MinIO'ya yükle
        result.write().format("csv").save("output.csv");

        minioClient.uploadObject(UploadObjectArgs.builder()
                .bucket("results")
                .object("output.csv")
                .filename("output.csv")
                .build());
    }
}