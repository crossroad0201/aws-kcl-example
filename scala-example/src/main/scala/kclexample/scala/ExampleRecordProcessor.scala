package kclexample.scala

import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClientBuilder }
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{ IRecordProcessor, IShutdownNotificationAware }
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.{ InitializationInput, ProcessRecordsInput, ShutdownInput }

import scala.annotation.tailrec
import scala.util.{ Failure, Success, Try }

class ExampleRecordProcessor(
  private val tableName: String
) extends IRecordProcessor with IShutdownNotificationAware {

  private var shardId: String = _
  private var dynamoDB: AmazonDynamoDB = _
  private var table: Table = _

  override def initialize(initializationInput: InitializationInput): Unit = {
    shardId = initializationInput.getShardId

    // Initialize any resources for #processRecords().
    dynamoDB = AmazonDynamoDBClientBuilder.defaultClient()
    table = new Table(dynamoDB, tableName)
  }

  override def processRecords(processRecordsInput: ProcessRecordsInput): Unit = {
    // Processing incoming records.
    retry {
      processRecordsInput.getRecords.forEach { record =>
        println(record)
      }
    }

    // Record checkpoint if all incoming records processed successfully.
    recordCheckpoint(processRecordsInput.getCheckpointer)
  }

  override def shutdown(shutdownInput: ShutdownInput): Unit = {
    // Record checkpoint at closing shard if shutdown reason is TERMINATE.
    if (shutdownInput.getShutdownReason == ShutdownReason.TERMINATE) {
      recordCheckpoint(shutdownInput.getCheckpointer)
    }

    // Cleanup initialized resources.
    Option(dynamoDB).foreach(_.shutdown())
  }

  override def shutdownRequested(checkpointer: IRecordProcessorCheckpointer): Unit = {
    // Record checkpoint at graceful shutdown.
    recordCheckpoint(checkpointer)
  }

  private def recordCheckpoint(checkpointer: IRecordProcessorCheckpointer): Unit = {
    retry {
      checkpointer.checkpoint()
    }
  }

  @tailrec
  private def retry(f: => Unit): Unit = {
    Try {
      f
    } match {
      case Success(_) => ()
      case Failure(e) =>
        println(s"An error occurred $e. That will be retry...")
        retry(f)
    }
  }

}
