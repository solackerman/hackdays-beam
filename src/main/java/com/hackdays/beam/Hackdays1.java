package com.hackdays.beam;

/**
 * Created by solackerman on 2016-06-09.
 */

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Arrays;

public class Hackdays1 {

    /**
     * Options supported by WordCount.
     * <p>
     * Inherits standard configuration options.
     */
    public interface Options extends PipelineOptions, FlinkPipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
        String getInput();
        void setInput(String value);

        @Description("Path of the file to write to")
        String getOutput();
        void setOutput(String value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(Options.class);

        options.setRunner(FlinkPipelineRunner.class);

        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
                .apply(FlatMapElements.via((String word) -> Arrays.asList(word.split("[^a-zA-Z']+")))
                        .withOutputType(new TypeDescriptor<String>() {}))
                .apply(Filter.byPredicate((String word) -> !word.isEmpty()))
                .apply(Count.<String>perElement())
                .apply(MapElements
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue())
                        .withOutputType(new TypeDescriptor<String>() {}))
                .apply(TextIO.Write.named("WriteCounts")
                        .to(options.getOutput()));

        p.run();
    }
}
