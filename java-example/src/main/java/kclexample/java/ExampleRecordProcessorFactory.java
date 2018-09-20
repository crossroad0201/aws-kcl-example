package kclexample.java;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class ExampleRecordProcessorFactory implements IRecordProcessorFactory {

    private final String tableName;

    ExampleRecordProcessorFactory(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new ExampleRecordProcessor(tableName);
    }
}
