package com.bucli.commands;

import com.bucli.exceptions.CommandExecutionException;
import com.bucli.validators.BucketValidator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import javax.inject.Inject;
import java.nio.file.Path;

@Command(name = "get", description = "Get file from S3 bucket")
public class Get implements Runnable {

    @Parameters(paramLabel = "<bucket name>", description = "Bucket destination name")
    private String bucketName;

    @Parameters(paramLabel = "<object name>", description = "The object name in S3")
    private String objectName;

    @Parameters(paramLabel = "<object name>", description = "The name of the downloaded file")
    private String resultFileName;

    @Inject
    protected S3Client s3Client;

    @Inject
    protected BucketValidator bucketValidator;

    @Override
    public void run() {
        try {
            bucketValidator.verifyIfBucketExists(bucketName);
            get();
        } catch (CommandExecutionException e) {
            System.err.println(">> " + e.getMessage());
        }
    }

    private void get() {
        GetObjectRequest request = GetObjectRequest.builder()
                .key(objectName)
                .bucket(bucketName)
                .build();
        s3Client.getObject(request, Path.of(resultFileName));
    }

}
