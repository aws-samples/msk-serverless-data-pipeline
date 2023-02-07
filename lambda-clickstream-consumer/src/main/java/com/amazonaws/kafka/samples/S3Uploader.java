package com.amazonaws.kafka.samples;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.ByteBuffer;


public class S3Uploader {

    static Random rand = new Random();
    Region region = Region.of(System.getenv("AWS_REGION"));

    S3Client s3 = S3Client.builder()
            .region(region)
            .build();

    void uploadS3File(String csvData) {
        int n = rand.nextInt(100000000);

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(System.getenv("BUCKET_NAME"))
                .key(String.format("csv/data-%d.csv", n))
                .build();

        s3.putObject(objectRequest, RequestBody.fromByteBuffer(ByteBuffer.wrap(csvData.getBytes(StandardCharsets.UTF_8))));
    }
}
