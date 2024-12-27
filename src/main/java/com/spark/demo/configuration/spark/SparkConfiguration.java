package com.spark.demo.configuration.spark;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfiguration {

    @Bean
    public SparkSession sparkSession() {
        try {
            // SparkSession oluşturuluyor
            SparkSession spark = SparkSession.builder()
                    .appName("Data Analysis App")
                    .master("local[*]") // Eğer bir Spark cluster kullanıyorsanız, burada cluster master URL'sini belirtin.
                    .getOrCreate();
            return spark;
        } catch (Exception e) {
            // SparkSession yaratılırken bir hata oluşursa burada yakalayabilirsiniz.
            throw new RuntimeException("Error initializing SparkSession", e);
        }
    }
}
