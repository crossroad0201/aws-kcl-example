package kclexample.java;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class Main {

    public static void main(String... args) {

        // Create a Worker.
        final Worker worker = new Worker.Builder()
                .recordProcessorFactory(
                        new ExampleRecordProcessorFactory("examples-table")
                )
                .config(
                        new KinesisClientLibConfiguration(
                                "kcl-java-example",
                                "kcl-sample",
                                DefaultAWSCredentialsProviderChain.getInstance(),
                                generateWorkerId()
                        ).withRegionName("us-east-1")
                        .withInitialLeaseTableReadCapacity(1)
                        .withInitialLeaseTableWriteCapacity(1)
                )
                .build();

        // Shutdown worker gracefully using shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                worker.startGracefulShutdown().get();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }));

        // Start the worker.
        worker.run();
    }

    private static String generateWorkerId() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Could not generate worker ID.", e);
        }
    }

}
