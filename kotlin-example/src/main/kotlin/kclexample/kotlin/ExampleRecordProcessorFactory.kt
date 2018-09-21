package kclexample.kotlin

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory

class ExampleRecordProcessorFactory(
        private val tableName: String
): IRecordProcessorFactory {

    override fun createProcessor(): IRecordProcessor {
        return ExampleRecordProcessor(tableName)
    }
}
