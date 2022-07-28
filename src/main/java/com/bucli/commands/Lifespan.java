package com.bucli.commands;

import com.bucli.exceptions.CommandExecutionException;
import com.bucli.validators.BucketValidator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import javax.inject.Inject;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

@Command(name = "lifespan", description = "Deletes all objects with 'last modification' date smaller than a given time lifespan")
public class Lifespan implements Runnable {

    @Parameters(paramLabel = "<bucket name>", description = "Bucket destination name")
    private String bucketName;

    @Parameters(paramLabel = "<lifespan>", description = "Lifespan applied over objects")
    private long lifespan;

    @Option(names = {"-u", "--unity"}, paramLabel = "<temporal unity>", description = "Temporal unity to be considered in lifespan", defaultValue = "DAYS", type = ChronoUnit.class)
    private ChronoUnit temporalUnity;

    @Option(names = {"-p", "--prefix"}, paramLabel = "<key prefix>", description = "Prefix rule to filter object keys")
    private String prefix;

    @Option(names = {"-s", "--suffix"}, paramLabel = "<key suffix>", description = "Suffix rule to filter object keys")
    private String suffix;

    @Inject
    protected S3Client s3Client;

    @Inject
    protected BucketValidator bucketValidator;

    @Override
    public void run() {
        try {
            bucketValidator.verifyIfBucketExists(bucketName);
            delete(generateIdentifiers());
        } catch (CommandExecutionException e) {
            System.err.println(">> " + e.getMessage());
        }
    }

    private List<ObjectIdentifier> generateIdentifiers() {
        ListObjectsRequest request = ListObjectsRequest.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .build();
        return s3Client.listObjects(request)
                .contents()
                .stream()
                .filter(obj -> isNull(suffix) || obj.key().endsWith(suffix))
                .filter(obj -> obj.lastModified().isBefore(Instant.now().minus(lifespan, temporalUnity)))
                .map(obj -> ObjectIdentifier.builder().key(obj.key()).build())
                .collect(Collectors.toList());
    }

    private void delete(List<ObjectIdentifier> identifiers) {
        if (identifiers.isEmpty()) {
            System.out.println(">> No file matches lifespan and rules");
            return;
        }
        Delete delete = Delete.builder()
                .objects(identifiers)
                .build();
        DeleteObjectsRequest request = DeleteObjectsRequest.builder()
                .bucket(bucketName)
                .delete(delete)
                .build();
        s3Client.deleteObjects(request);
    }

}
