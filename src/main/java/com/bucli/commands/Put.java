package com.bucli.commands;

import com.bucli.exceptions.CommandExecutionException;
import com.bucli.validators.BucketValidator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.inject.Inject;
import java.nio.file.Path;

import static java.util.Objects.requireNonNullElse;

@Command(name = "put", description = "Send file to S3 bucket")
public class Put implements Runnable {

    @Parameters(paramLabel = "<bucket name>", description = "Bucket destination name")
    private String bucketName;

    @Parameters(paramLabel = "<file>", description = "File to send to S3")
    private String filePath;

    @Option(names = {"-n", "--name"}, description = "The name of the file in S3. Use this option to assign a file path: folder/file.txt")
    private String keyName;

    @Inject
    protected S3Client s3Client;

    @Inject
    protected BucketValidator bucketValidator;

    @Override
    public void run() {
        try {
            Path path = Path.of(filePath);
            verifyIfFileExists(path);
            keyName = requireNonNullElse(keyName, path.getFileName().toString());
            bucketValidator.verifyIfBucketExists(bucketName);
            putFile(path);
        } catch (CommandExecutionException e) {
            System.err.println(">> " + e.getMessage());
        }
    }

    private void verifyIfFileExists(Path path) {
        if (!path.toFile().exists()) {
            throw new CommandExecutionException("File " + filePath + " does not exists");
        }
    }

    private void putFile(Path path) {
        var putS3Request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();
        s3Client.putObject(putS3Request, path);
    }

}
