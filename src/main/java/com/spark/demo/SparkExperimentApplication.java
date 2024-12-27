package com.spark.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.spark.demo"})
public class SparkExperimentApplication {

	public static void main(String[] args) {
		SpringApplication.run(SparkExperimentApplication.class, args);
	}

}
