package kclexample.scala

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory

class ExampleRecordProcessorFactory(
  private val tableName: String
) extends IRecordProcessorFactory {

  override def createProcessor() = new ExampleRecordProcessor(tableName)

}
