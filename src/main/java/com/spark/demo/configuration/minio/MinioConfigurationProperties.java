package com.spark.demo.configuration.minio;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Component
@ConfigurationProperties(prefix = "minio")
public class MinioConfigurationProperties {
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private String bucketName;

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }
}