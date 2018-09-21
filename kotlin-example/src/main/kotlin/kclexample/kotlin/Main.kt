package kclexample.kotlin

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.*

fun main(args: Array<String>) {

    // Create a Worker.
    val worker = Worker.Builder()
            .recordProcessorFactory(
                    ExampleRecordProcessorFactory("examples-table")
            )
            .config(
                    KinesisClientLibConfiguration(
                            "kcl-kotlin-example",
                            "kcl-sample",
                             DefaultAWSCredentialsProviderChain.getInstance(),
                             generateWorkerId()
                    ).withRegionName("us-east-1")
                    .withInitialLeaseTableReadCapacity(1)
                    .withInitialLeaseTableWriteCapacity(1)
            )
            .build()

    // Shutdown worker gracefully using shutdown hook.
    Runtime.getRuntime().addShutdownHook(Thread {
        try {
            worker.startGracefulShutdown().get()
        } catch (e: Throwable) {
            e.printStackTrace()
        }
    })

    // Start the worker.
    worker.run()
}

fun generateWorkerId(): String {
    try {
        return InetAddress.getLocalHost().canonicalHostName + ":" + UUID.randomUUID()
    } catch (e: UnknownHostException) {
        throw RuntimeException("Could not generate worker ID.", e)
    }
}
