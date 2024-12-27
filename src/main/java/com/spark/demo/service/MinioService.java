package com.spark.demo.service;

import io.minio.messages.Bucket;

import java.io.InputStream;
import java.util.List;

public interface MinioService {

    void uploadFile(final String objectName, final InputStream stream, final String contentType) throws Exception;

    InputStream downloadFile(final String objectName) throws Exception;

    void deleteFile(final String objectName) throws Exception;

    List<Bucket> getAllBuckets() throws Exception;

}
