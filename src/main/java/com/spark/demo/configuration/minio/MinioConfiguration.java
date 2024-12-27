package com.spark.demo.configuration.minio;

import io.minio.MinioClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MinioConfiguration {

    private final MinioConfigurationProperties minioConfigurationProperties;

    public MinioConfiguration(MinioConfigurationProperties minioConfigurationProperties) {
        this.minioConfigurationProperties = minioConfigurationProperties;
    }

    @Bean
    public MinioClient minioClient() {
        return MinioClient.builder()
                .endpoint(minioConfigurationProperties.getEndpoint())
                .credentials(minioConfigurationProperties.getAccessKey(), minioConfigurationProperties.getSecretKey())
                .build();
    }

}

