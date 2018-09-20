package kclexample.java;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

import java.util.Optional;

public class ExampleRecordProcessor implements IRecordProcessor, IShutdownNotificationAware {

    private final String tableName;

    ExampleRecordProcessor(String tableName) {
        this.tableName = tableName;
    }

    private String shardId;
    private AmazonDynamoDB dynamoDB;
    private Table table;

    @Override
    public void initialize(InitializationInput initializationInput) {
        shardId = initializationInput.getShardId();

        // Initialize any resources for #processRecords().
        dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
        table = new Table(dynamoDB, tableName);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        // Processing incoming records.
        retry(() -> {
            processRecordsInput.getRecords().forEach(record -> {
                System.out.println(record);
            });
        });

        // Record checkpoint if all incoming records processed successfully.
        recordCheckpoint(processRecordsInput.getCheckpointer());
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        // Record checkpoint at closing shard if shutdown reason is TERMINATE.
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            recordCheckpoint(shutdownInput.getCheckpointer());
        }

        // Cleanup initialized resources.
        Optional.ofNullable(dynamoDB).ifPresent(AmazonDynamoDB::shutdown);
    }

    @Override
    public void shutdownRequested(IRecordProcessorCheckpointer checkpointer) {
        // Record checkpoint at graceful shutdown.
        recordCheckpoint(checkpointer);
    }

    private void recordCheckpoint(IRecordProcessorCheckpointer checkpointer) {
        retry(() -> {
            try {
                checkpointer.checkpoint();
            } catch (Throwable e) {
                throw new RuntimeException("Record checkpoint failed.", e);
            }
        });
    }

    private void retry(Runnable f) {
        try {
            f.run();
        } catch (Throwable e) {
            System.out.println(String.format("An error occurred %s. That will be retry...", e.getMessage()));
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e2) {
                e2.printStackTrace();
            }
            retry(f);
        }
    }

}
