package com.spark.demo.controller;

import com.spark.demo.service.MinioService;
import io.minio.messages.Bucket;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;

@RestController
@RequestMapping("/api/minio")
@RequiredArgsConstructor
public class MinIOController {

    @Autowired
    MinioService minioService;

    @GetMapping(path = "/buckets")
    // Tüm bucketları dönderir.
    public List<Bucket> getAllBuckets() throws Exception {
        return minioService.getAllBuckets();
    }

    /**
     * Endpoint to upload a file to MinIO.
     *
     * @param file The file to upload.
     * @return Success or error response.
     */
    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile file) {
        try {
            minioService.uploadFile(file.getOriginalFilename(), file.getInputStream(), file.getContentType());
            return ResponseEntity.ok("File uploaded successfully: " + file.getOriginalFilename());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error uploading file: " + e.getMessage());
        }
    }

    /**
     * Endpoint to download a file from MinIO.
     *
     * @param objectName The name of the file to download.
     * @return The file as a ResponseEntity.
     */
    @GetMapping("/download/{objectName}")
    public ResponseEntity<byte[]> downloadFile(@PathVariable String objectName) {
        try (InputStream stream = minioService.downloadFile(objectName);
             ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[1024];
            int bytesRead;

            // InputStream'den baytları okuyup ByteArrayOutputStream'e yazıyoruz.
            while ((bytesRead = stream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, bytesRead);
            }

            byte[] fileContent = byteArrayOutputStream.toByteArray();

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            headers.setContentDispositionFormData("attachment", objectName);

            return new ResponseEntity<>(fileContent, headers, HttpStatus.OK);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }


    /**
     * Endpoint to delete a file from MinIO.
     *
     * @param objectName The name of the file to delete.
     * @return Success or error response.
     */
    @DeleteMapping("/delete/{objectName}")
    public ResponseEntity<String> deleteFile(@PathVariable String objectName) {
        try {
            minioService.deleteFile(objectName);
            return ResponseEntity.ok("File deleted successfully: " + objectName);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error deleting file: " + e.getMessage());
        }
    }
}

