package com.spark.demo.service;

import io.minio.errors.*;
import io.minio.messages.Bucket;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public interface MinioService {

    void uploadFile(final String objectName, final InputStream stream, final String contentType) throws Exception;

    InputStream downloadFile(final String objectName) throws Exception;

    void deleteFile(final String objectName) throws Exception;

    List<Bucket> getAllBuckets() throws Exception;

    void downloadFileLocale(final String objectName, final String localFilePath);

    void uploadObject(final String bucket, final String object, final String fileName) throws IOException, ServerException, InsufficientDataException, ErrorResponseException, NoSuchAlgorithmException, InvalidKeyException, InvalidResponseException, XmlParserException, InternalException;

}
