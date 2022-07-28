package com.bucli.validators;

import com.bucli.exceptions.CommandExecutionException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class BucketValidator {

    @Inject
    protected S3Client s3Client;

    public void verifyIfBucketExists(String bucketName) {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();
        try {
            s3Client.headBucket(headBucketRequest);
        } catch (Exception e) {
            throw new CommandExecutionException("Bucket " + bucketName + " does not exists or you cannot access it");
        }
    }

}
