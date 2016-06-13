package com.hackdays.beam;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.repackaged.com.google.common.base.MoreObjects;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Objects;

/**
 * Created by solackerman on 2016-06-09.
 */

public class DateDimension {

    public interface Options extends PipelineOptions, FlinkPipelineOptions {

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

        p.apply(CountingInput.upTo(100000))
                .apply(MapElements
                        .via((Long i) -> new DateTime(0).toDateTime(DateTimeZone.UTC).plusDays(i.intValue()))
                        .withOutputType(new TypeDescriptor<DateTime>() {}))
                .apply(MapElements
                        .via((DateTime t) -> new DateRecord(t))
                        .withOutputType(new TypeDescriptor<DateRecord>() {}))
                .apply(AvroIO.Write.named("WriteAvro")
                        .to(options.getOutput())
                        .withoutSharding()
                        .withSchema(DateRecord.class));

        p.run();
    }
}

@DefaultCoder(AvroCoder.class)
class DateRecord {
    int _date_key;
    String datetime;

    public DateRecord(){}

    public DateRecord(DateTime t) {
        this._date_key = (t.getYear() * 10000) + (t.getMonthOfYear() * 100)+t.getDayOfMonth();
        this.datetime = t.toString("yyyy-mm-dd");
    }
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("_date_key", _date_key)
                .add("datetime", datetime)
                .toString();
    }
    @Override
    public int hashCode() {
        return Objects.hash(_date_key, datetime);
    }
    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof DateRecord)) {
            return false;
        }
        DateRecord o = (DateRecord) other;
        return Objects.equals(_date_key, o._date_key) && Objects.equals(datetime, o.datetime);
    }
}

