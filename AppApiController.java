package com.ws.fantasy.controller;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ws.fantasy.dto.ui.CreateTeamDTO;
import com.ws.fantasy.services.AppAPiService;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

@Slf4j
@RestController
@RequestMapping("/fantasy/api/v1/app")
public class AppApiController {


    private final int PART_SIZE = 5 * 1024 * 1024; // 5 MB

    @Autowired
    AppAPiService appService;

    @Autowired
    S3Client s3Client;

    @PostMapping("/match-players/{matchId}")
    public List<Map> getPlayerForMatch(@PathVariable(name = "matchId") String matchId) {
        return appService.getPlayerForMatch(matchId);
    }

    @PostMapping("/create-team")
    public String createTeam(@RequestBody(required = true) CreateTeamDTO teamDto) {
        return appService.createTeam(teamDto);
    }

    @PostMapping("/test")
    public String test() {


        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket("txph-dev-cj-1")
                .key("Head-First-Java-2nd.pdf")
                .build();

        //Path path = Paths.get("C:\\Users\\Chiranjit\\Documents\\CARD\\C1\\Head-First-Java-2nd.pdf");
        //GetObjectResponse getObjectResponse = s3Client.getObject(getObjectRequest, path);
        //System.out.println("File downloaded: " + getObjectResponse);

        ResponseBytes<GetObjectResponse> objectAsBytes = s3Client.getObjectAsBytes(getObjectRequest);
        String base64Encoded = Base64.getEncoder().encodeToString(objectAsBytes.asByteArray());
    return  base64Encoded;
       /* try (InputStream inputStream = s3Client.getObject(getObjectRequest);
             ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[4096];
            int bytesRead;

            // Read the file's input stream into a byte array
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, bytesRead);
            }

            byte[] fileBytes = byteArrayOutputStream.toByteArray();

            // Encode the byte array to a Base64 string
            String base64Encoded = Base64.getEncoder().encodeToString(objectAsBytes.asByteArray());
           // System.out.println(base64Encoded);

            // Decode the Base64 string to a byte array
            byte[] decodedBytes = Base64.getDecoder().decode(base64Encoded);

          *//*  String filePath="C:\\Users\\Chiranjit\\Documents\\CARD\\C1\\t1.pdf";
            // Write the byte array to a file
            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                fos.write(decodedBytes);
            }

            System.out.println("File saved successfully to " + filePath);*//*
            return base64Encoded;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/

        //downloadFileAsBase64("txph-dev-cj-1","Head-First-Java-2nd.pdf");

        //return  downloadFileAsBase64("txph-dev-cj-1","Head-First-Java-2nd.pdf");
    }


    public String downloadFileAsBase64(String bucketName, String key) {

        try {
            // Get the file size
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            HeadObjectResponse headResponse = s3Client.headObject(headRequest);
            long fileSize = headResponse.contentLength();

            // Calculate the number of parts
            int partCount = (int) (fileSize / PART_SIZE) + (fileSize % PART_SIZE == 0 ? 0 : 1);
            ExecutorService executor = Executors.newFixedThreadPool(partCount);

            List<Future<byte[]>> futures = new ArrayList<>();
            for (int i = 0; i < partCount; i++) {
                long start = i * PART_SIZE;
                long end = Math.min(start + PART_SIZE - 1, fileSize - 1);
                futures.add(executor.submit(new S3PartDownloader(bucketName, key, start, end)));
            }

            // Combine parts
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            for (Future<byte[]> future : futures) {
                outputStream.write(future.get());
            }

            executor.shutdown();
            log.info("Size :"+outputStream.size());
            // Encode the combined byte array to a Base64 string
            String base64Encoded = Base64.getEncoder().encodeToString(outputStream.toByteArray());
            log.info("Successfully converted file to Base64");

          /*  byte[] decodedBytes = Base64.getDecoder().decode(base64Encoded);

            String filePath="C:\\Users\\Chiranjit\\Documents\\CARD\\C1\\t1.pdf";
            // Write the byte array to a file
            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                fos.write(decodedBytes);
            }

           log.info("File saved successfully to " + filePath);*/

            return base64Encoded;

        } catch (S3Exception e) {
            log.error("S3 error: " + e.awsErrorDetails().errorMessage(), e);
        } catch (IOException | InterruptedException | ExecutionException e) {
            log.error("Error during multi-part download", e);
        }
        return null;
    }


    private class S3PartDownloader implements Callable<byte[]> {
        private final String bucketName;
        private final String key;
        private final long start;
        private final long end;

        public S3PartDownloader(String bucketName, String key, long start, long end) {
            this.bucketName = bucketName;
            this.key = key;
            this.start = start;
            this.end = end;
        }

        @Override
        public byte[] call() throws Exception {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .range("bytes=" + start + "-" + end)
                    .build();

            try (InputStream inputStream = s3Client.getObject(getObjectRequest);
                 ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {

                byte[] buffer = new byte[4096];
                int bytesRead;

                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    byteArrayOutputStream.write(buffer, 0, bytesRead);
                }

                return byteArrayOutputStream.toByteArray();

            } catch (IOException e) {
                log.error("IO error while downloading part", e);
                throw new RuntimeException("Error downloading part", e);
            }
        }
    }
}
