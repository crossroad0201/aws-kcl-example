package kclexample.scala

import java.net.InetAddress
import java.util.UUID

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{ KinesisClientLibConfiguration, Worker }

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }
import scala.util.{ Failure, Success, Try }

object Main extends App {

  import scala.concurrent.ExecutionContext.Implicits.global

  // Create a Worker.
  val worker = new Worker.Builder()
    .recordProcessorFactory(
      new ExampleRecordProcessorFactory("examples-table")
    )
    .config(
      new KinesisClientLibConfiguration(
        "kcl-scala-example",
        "kcl-sample",
        DefaultAWSCredentialsProviderChain.getInstance(),
        generateWorkerId()
      ).withRegionName("us-east-1")
        .withInitialLeaseTableReadCapacity(1)
        .withInitialLeaseTableWriteCapacity(1)
    )
    .build()

  // Shutdown worker gracefully using shutdown hook.
  sys.addShutdownHook(
    Await.result(
      Future(worker.startGracefulShutdown().get()),
      Duration.Inf
    )
  )

  // Start the worker.
  worker.run()

  def generateWorkerId(): String = {
    Try {
      InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()
    } match {
      case Success(workerId) => workerId
      case Failure(e) => throw new RuntimeException("Could not generate worker ID.", e)
    }
  }

}
