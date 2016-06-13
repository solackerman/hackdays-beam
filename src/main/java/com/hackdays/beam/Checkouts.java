package com.hackdays.beam;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineRunner;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedFlinkSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.*;

/**
 * Created by solackerman on 2016-06-10.
 */
public class Checkouts {
//    public interface Options extends FlinkPipelineOptions {
//
//        @Description("Path of the file to write to")
//        String getOutput();
//        void setOutput(String value);
//    }

    /**
     * Custom options for the Pipeline
     */
    public interface Options extends PipelineOptions, FlinkPipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
        String getInput();
        void setInput(String value);

        @Description("Path of the file to write to")
        String getOutput();
        void setOutput(String value);

        @Description("Sliding window duration, in seconds")
        @Default.Long(60)
        Long getWindowSize();

        void setWindowSize(Long value);

        @Description("The Kafka topic to read from")
        @Default.String("checkout")
        String getKafkaTopic();

        void setKafkaTopic(String value);

        @Description("The Kafka Broker to read from")
        @Default.String("kafka04.chi.shopify.com:9092,kafka05.chi.shopify.com:9092,kafka06.chi.shopify.com:9092,kafka07.chi.shopify.com:9092,kafka08.chi.shopify.com:9092,kafka09.chi.shopify.com:9092,kafka10.chi.shopify.com:9092")
        String getBroker();

        void setBroker(String value);

        @Description("The Zookeeper server to connect to")
        @Default.String("zk1.chi.shopify.com:2170,zk4.chi.shopify.com:2170,zk5.chi.shopify.com:2170")
        String getZookeeper();

        void setZookeeper(String value);

        @Description("The groupId")
        @Default.String("sol_be_hacking")
        String getGroup();

        void setGroup(String value);
    }


    public static class FormatAsStringFn extends DoFn<KV<Integer, Long>, String>{
        @Override
        public void processElement(ProcessContext c) {
            //c.window().maxTimestamp().toString() + "@ "
            String row =  c.timestamp().toString() + ", " + c.element().getKey() + ": " + c.element().getValue();
            System.out.println(row);
            c.output(row);
        }
    }



    public static void main(String[] args) {
        // PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .as(Options.class);
        options.setJobName("KafkaExample - WindowSize: " + options.getWindowSize() + " seconds");
        options.setStreaming(true);
        options.setCheckpointingInterval(1000L);
        options.setNumberOfExecutionRetries(5);
        options.setExecutionRetryDelay(3000L);
        options.setRunner(FlinkPipelineRunner.class);


        //System.out.println(options.getKafkaTopic() +" "+ options.getZookeeper() +" "+ options.getBroker() +" "+ options.getGroup() );
        Pipeline pipeline = Pipeline.create(options);

        Properties p = new Properties();
        p.setProperty("zookeeper.connect", options.getZookeeper());
        p.setProperty("bootstrap.servers", options.getBroker());
        p.setProperty("group.id", options.getGroup());

        // this is the Flink consumer that reads the input to
        // the program from a kafka topic.
        FlinkKafkaConsumer08<String> kafkaConsumer = new FlinkKafkaConsumer08<>(
                options.getKafkaTopic(),
                new SimpleStringSchema(), p);

        Long WINDOW = options.getWindowSize();
        Set<Integer> SHOPS = new HashSet();
        SHOPS.addAll(Arrays.asList(13247915, 942252, 499112));
        Long MAX_LATENESS = 2L;


        pipeline.apply(Read.named("StreamingWordCount")
                        .from(UnboundedFlinkSource.of(kafkaConsumer)))
                .apply(MapElements.via(
                        (String s) -> {try {return new ObjectMapper().readValue(s, HashMap.class);
                                            }catch(IOException e){return null;}})
                        .withOutputType(new TypeDescriptor<HashMap>() {}))
                .apply("filter missing info", Filter.byPredicate((HashMap m) -> m != null
                        && m.containsKey("shop_id") && m.containsKey("event_timestamp")
                        && SHOPS.contains(m.get("shop_id"))
                        // && !Instant.parse((String) m.get("event_timestamp")).isBefore(Instant.now().minus(Duration.standardMinutes(2)))
                ))
                .apply("add timestamps", WithTimestamps.of((HashMap m) -> Instant.parse((String) m.get("event_timestamp")))

                        .withAllowedTimestampSkew(Duration.standardHours(MAX_LATENESS))
                )
                .apply("shop id", MapElements.via((HashMap m) -> KV.of((Integer) m.get("shop_id"), 1L))
                        .withOutputType(new TypeDescriptor<KV<Integer, Long>>() {}))
                .apply("Window", Window.<KV<Integer, Long>>into(FixedWindows.of(Duration.standardSeconds(WINDOW)))
                        .triggering(AfterWatermark.pastEndOfWindow()
                                .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
                                                                    .plusDelayOf(Duration.standardSeconds(WINDOW)))
                        )
                        .withAllowedLateness(Duration.standardHours(MAX_LATENESS))
                        .accumulatingFiredPanes()
                )
                .apply("count", Sum.longsPerKey())
                .apply("filter > 1", Filter.byPredicate((KV<Integer, Long> i) -> i.getValue() > 1 ))
                .apply("stringify", ParDo.of(new FormatAsStringFn()))
                .apply("output", TextIO.Write.to("./outputKafka.txt")
                );

        pipeline.run();
    }
}

// windowed total every hour/shop, filtered by shops with over 100 orders