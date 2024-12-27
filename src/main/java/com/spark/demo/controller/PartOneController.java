package com.spark.demo.controller;

import com.spark.demo.service.DataProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/partone")
@RequiredArgsConstructor
public class PartOneController {

    @Autowired
    private DataProcessingService dataProcessingService;

    @GetMapping("/processData")
    public String processData() {
        try {
            dataProcessingService.processData();
            return "Data processed and uploaded successfully.";
        } catch (Exception e) {
            return "Error during data processing: " + e.getMessage();
        }
    }

    @GetMapping("/processAndUploadData")
    public String processAndUploadData() {
        try {
            dataProcessingService.processAndUploadData();
            return "Data processed and uploaded successfully.";
        } catch (Exception e) {
            return "Error during data processing: " + e.getMessage();
        }
    }
}
