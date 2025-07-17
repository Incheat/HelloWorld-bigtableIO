package com.google.cloud.bigtable.dataflow.example;

import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableWriteResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.Pipeline;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import java.util.*;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class HelloWorldWrite {

    private static final Logger LOG = LoggerFactory.getLogger(HelloWorldWrite.class);

    private static final byte[] FAMILY = Bytes.toBytes("cf1");
    private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
    private static final byte[] VALUE = Bytes.toBytes("value_" + (60 * Math.random()));

    static final DoFn<String, KV<ByteString, Iterable<Mutation>>> MUTATION_TRANSFORM = new DoFn<String, KV<ByteString, Iterable<Mutation>>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String input = c.element();

            ByteString rowKey = ByteString.copyFromUtf8(input.equals("test") ? createLargeRowKeyString() : input);
      
            Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                .setFamilyName("cf1")
                .setColumnQualifier(ByteString.copyFromUtf8("column"))
                .setValue(ByteString.copyFromUtf8(input))
                .build();
        
            c.output(KV.of(rowKey, Collections.singletonList(Mutation.newBuilder().setSetCell(cell).build())));
        }
    };

    static final DoFn<KV<ByteString, Iterable<Mutation>>, KV<ByteString, Iterable<Mutation>>> TYPE_CHECKER = new DoFn<KV<ByteString, Iterable<Mutation>>, KV<ByteString, Iterable<Mutation>>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Object key = c.element().getKey();
            Object val = c.element().getValue();
            boolean noError = true;

            if (!(key instanceof ByteString)) {
                LOG.error("DEBUG ⚠️ Not ByteString Key: " + key.getClass().getName());
                noError = false;
            }

            if (!(val instanceof Iterable)) {
                LOG.error("DEBUG ⚠️ Not Iterable Value: " + val.getClass().getName());
                noError = false;
            }

            if (noError) {
                LOG.info("DEBUG: Types are all correct");
            }

            c.output(c.element());
        }
    };

    static final DoFn<BigtableWriteResult, BigtableWriteResult> WRITE_RESULT_CHECKER = new DoFn<BigtableWriteResult, BigtableWriteResult>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
          LOG.info("DEBUG (written number): " + c.element().getRowsWritten());
          c.output(c.element());
      }
    };

    static final DoFn<String, String> DUMMY_PROCESSOR = new DoFn<String, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String input = c.element();
            LOG.info("DEBUG Test: " + input);
            c.output("Final test processed: " + input);
        }
    };

    static String createLargeRowKeyString() {
      char[] chars = new char[5000]; // > 4KB, https://cloud.google.com/bigtable/quotas#limits-data-size
      Arrays.fill(chars, 'a');
      return new String(chars);
    }

    public static void main(String[] args) {

        CloudBigtableOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(CloudBigtableOptions.class);
        SdkHarnessOptions loggingOptions = options.as(SdkHarnessOptions.class);
        // Overrides the default log level on the worker to emit logs at TRACE or higher.
        loggingOptions.setDefaultSdkHarnessLogLevel(SdkHarnessOptions.LogLevel.TRACE);
        
        Pipeline pipeline = Pipeline.create(options);

        String PROJECT_ID = options.getBigtableProjectId();
        String INSTANCE_ID = options.getBigtableInstanceId();
        String TABLE_ID = options.getBigtableTableId();

        PCollection<BigtableWriteResult> writeResults = 
            pipeline.apply("CreateSimpleRows", Create.of("hello"))
                .apply("ToBigtableRows", ParDo.of(MUTATION_TRANSFORM))
                // .apply("DebugTypeCheck", ParDo.of(TYPE_CHECKER))
                .apply("WriteToBigtable", BigtableIO.write()
                    .withProjectId(PROJECT_ID)
                    .withInstanceId(INSTANCE_ID)
                    .withTableId(TABLE_ID)
                    .withWriteResults());
        
        writeResults
            .apply("wait for writes", Wait.on(writeResults))
            .apply("DebugWriteResults", ParDo.of(WRITE_RESULT_CHECKER));

        pipeline.run();
    }
}

