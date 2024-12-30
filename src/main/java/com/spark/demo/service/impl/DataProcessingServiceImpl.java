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

import java.io.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


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

    @Override
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

        personDF.show();

        Dataset<Row> countryDF = sparkSession.read().format("csv")
                .option("header", "true")
                .load(countryDataPath);

        countryDF.show();

        // Yaş hesaplama ve kan grubu filtreleme
        Dataset<Row> filtered = personDF.filter(personDF.col("birthday").isNotNull())
                .withColumn("age", ageFromBirthday(personDF.col("birthday"))) // Yaş hesaplama
                .filter(col("age").gt(30)) // 30 yaş üstü
                .filter(col("blood_type").isin("A+", "A-", "AB+", "AB-")); // Kan grubu filtreleme

        filtered.show();

        Dataset<Row> joined = filtered.join(countryDF, "country");

        Dataset<Row> result = joined.groupBy("country_name")
                .agg(
                        count("country").alias("count"),
                        functions.concat_ws(", ", functions.collect_list("first_name")).alias("names") // names sütununu düz metin olarak birleştir
                );

        result.show();
        writeDatasetToExcel(result, "C:/Erbin/data/output.xlsx");

        // İşlenmiş veriyi yazma
//        result.coalesce(1) // Tek bir dosya yapmak için partisyonları birleştir
//                .write()
//                .mode("overwrite") // Var olan dizini sil ve üzerine yaz
//                .option("header", "true") // Başlık satırını dahil et
//                .csv("C:/Erbin/data/output.csv");

        // Upload result to MinIO
        minioClient.uploadObject(
                io.minio.UploadObjectArgs.builder()
                        .bucket("results")
                        .object("output.xlsx")
                        .filename("C:/Erbin/data/output.xlsx")
                        .build()
        );
    }

    // Yaş hesaplamak için fonksiyon
    public static Column ageFromBirthday(Column birthday) {
        return expr("datediff(current_date(), to_date(" + birthday + ", 'dd-MM-yyyy')) / 365");
    }

    public static void writeDatasetToExcel(Dataset<Row> dataset, String filePath) throws IOException {
        // Excel dosyasını oluştur
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Data");

        // Dataset'ten başlıkları al
        String[] columns = dataset.columns();

        // Başlık satırını ekle
        org.apache.poi.ss.usermodel.Row headerRow = sheet.createRow(0);
        for (int i = 0; i < columns.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(columns[i]);
        }

        // Verileri satırlara yaz
        int rowNum = 1;
        for (Row row : dataset.collectAsList()) {
            org.apache.poi.ss.usermodel.Row sheetRow = sheet.createRow(rowNum++);
            for (int i = 0; i < row.length(); i++) {
                Cell cell = sheetRow.createCell(i);
                Object cellValue = row.get(i);

                // Farklı veri türleri için uygun hücre değeri ayarlama
                if (cellValue instanceof String) {
                    cell.setCellValue((String) cellValue);
                } else if (cellValue instanceof Integer) {
                    cell.setCellValue((Integer) cellValue);
                } else if (cellValue instanceof Double) {
                    cell.setCellValue((Double) cellValue);
                } else if (cellValue instanceof Boolean) {
                    cell.setCellValue((Boolean) cellValue);
                } else {
                    cell.setCellValue(cellValue != null ? cellValue.toString() : "");
                }
            }
        }

        // Excel dosyasını kaydet
        try (FileOutputStream fileOut = new FileOutputStream(filePath)) {
            workbook.write(fileOut);
        }

        // Kaynakları temizle
        workbook.close();
    }


}