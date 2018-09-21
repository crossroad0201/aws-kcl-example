package kclexample.kotlin

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput

class ExampleRecordProcessor(
        private val tableName: String
) : IRecordProcessor, IShutdownNotificationAware {

    private var shardId: String? = null
    private var dynamoDB: AmazonDynamoDB? = null
    private var table: Table? = null

    override fun initialize(initializationInput: InitializationInput) {
        shardId = initializationInput.shardId

        // Initialize any resources for #processRecords().
        dynamoDB = AmazonDynamoDBClientBuilder.defaultClient()
        table = Table(dynamoDB, tableName)
    }

    override fun processRecords(processRecordsInput: ProcessRecordsInput) {
        // Processing incoming records.
        retry {
            processRecordsInput.records.forEach { record ->
                println(record)
            }
        }

        // Record checkpoint if all incoming records processed successfully.
        recordCheckpoint(processRecordsInput.checkpointer)
    }

    override fun shutdown(shutdownInput: ShutdownInput) {
        // Record checkpoint at closing shard if shutdown reason is TERMINATE.
        if (shutdownInput.shutdownReason == ShutdownReason.TERMINATE) {
            recordCheckpoint(shutdownInput.checkpointer)
        }

        // Cleanup initialized resources.
        dynamoDB?.shutdown()
    }

    override fun shutdownRequested(checkpointer: IRecordProcessorCheckpointer) {
        // Record checkpoint at graceful shutdown.
        recordCheckpoint(checkpointer)
    }

    private fun recordCheckpoint(checkpointer: IRecordProcessorCheckpointer) {
        retry {
            checkpointer.checkpoint()
        }
    }

    private fun retry(f: () -> Unit) {
        try {
            f()
        } catch (e: Throwable) {
            println("An error occurred $e. That will be retry...")
            Thread.sleep(3000)
            retry(f)
        }
    }

}