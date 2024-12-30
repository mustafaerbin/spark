package com.spark.demo.service.impl;

import com.spark.demo.configuration.minio.MinioConfigurationProperties;
import com.spark.demo.service.MinioService;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.errors.*;
import io.minio.messages.Bucket;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@Service
@Transactional
public class MinioServiceImpl implements MinioService {

    private final MinioClient minioClient;
    private final MinioConfigurationProperties minioConfigurationProperties;

    @Autowired
    public MinioServiceImpl(
            MinioClient minioClient, MinioConfigurationProperties minioConfigurationProperties) {
        this.minioClient = minioClient;
        this.minioConfigurationProperties = minioConfigurationProperties;
    }

    @Override
    public void uploadFile(String objectName, InputStream stream, String contentType) throws Exception {

        try {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(minioConfigurationProperties.getBucketName())
                            .object(objectName)
                            .stream(stream, stream.available(), -1)
                            .contentType(contentType)
                            .build());
        } catch (MinioException e) {
            throw new Exception("Error occurred while uploading file", e);
        }
    }

    @Override
    public InputStream downloadFile(String objectName) throws Exception {

        try {
            return minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(minioConfigurationProperties.getBucketName())
                            .object(objectName)
                            .build());
        } catch (MinioException e) {
            throw new Exception("Error occurred while downloading file", e);
        }
    }

    @Override
    public void deleteFile(String objectName) throws Exception {
        try {
            minioClient.removeObject(
                    RemoveObjectArgs.builder()
                            .bucket(minioConfigurationProperties.getBucketName())
                            .object(objectName)
                            .build());
        } catch (MinioException e) {
            throw new Exception("Error occurred while deleting file", e);
        }
    }

    @Override
    public List<Bucket> getAllBuckets() throws Exception {
        return minioClient.listBuckets();
    }

    @Override
    public void downloadFileLocale(String objectName, String localFilePath) {
        try (InputStream stream = downloadFile(objectName)) {
            if (!Files.exists(Paths.get(localFilePath)))
                Files.copy(stream, Paths.get(localFilePath));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void uploadObject(String bucket, String object, String fileName) throws IOException, ServerException, InsufficientDataException, ErrorResponseException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException {
        minioClient.uploadObject(
                io.minio.UploadObjectArgs.builder()
                        .bucket(bucket)
                        .object(object)
                        .filename(fileName)
                        .build());
    }
}

