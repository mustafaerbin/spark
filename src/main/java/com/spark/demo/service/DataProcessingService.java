package com.spark.demo.service;

import java.io.IOException;

public interface DataProcessingService {

    void processData() throws Exception;

    void processAndUploadData() throws RuntimeException, IOException;

}
