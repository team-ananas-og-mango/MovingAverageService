/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package ema.ema;

import org.apache.flink.util.Collector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.api.common.functions.MapFunction;
import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import javax.annotation.Nullable;
/**
 *
 * @author madsfalken
 */
public class Ema {

    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics("xcse")
            .setGroupId("flink-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        
        DataStream<String> incoming = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource");

        // Deserializing incoming Kafka messages to StockMeasurement objects
        DataStream<StockMeasurement> measurements = incoming.map(new MapFunction<String, StockMeasurement>() {
            @Override
            public StockMeasurement map(String value) throws Exception {
                // Use your favorite library to deserialize JSON (like Gson, Jackson, etc.)
                // Assuming value is a JSON string representation of a StockMeasurement object
                return new Gson().fromJson(value, StockMeasurement.class);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<StockMeasurement>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.time)));

        // Keying the stream by the 'symbol' field
        KeyedStream<StockMeasurement, String> stockKeyedStream = measurements.keyBy(stockMeasurement -> stockMeasurement.symbol);

        // Processing the stream with your EMACalculator and specifying the output type
        SingleOutputStreamOperator<EmaStream> stream = stockKeyedStream.process(new EMACalculator());
        
        KafkaSink<EmaStream> sink = KafkaSink
            .<EmaStream>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(new EmaStreamSerializationSchema())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        stream.sinkTo(sink);
    }
    
    public static class StockMeasurement {
        public String symbol;
        public float close;
        public float high;
        public float interest;
        public float low;
        public float open;
        public String time;
        public float volume;
    }

private static class EMACalculator extends KeyedProcessFunction<String, StockMeasurement, EmaStream> {

        private transient ValueState<Float> emaValue10State;
        private transient ValueState<Float> emaValue100State;

        /**
         * On operator creation the steps to be run
         * @param configuration
         */
        @Override
        public void open(Configuration configuration) {
            ValueStateDescriptor<Float> ema10State = new ValueStateDescriptor<>(
                    "ema10", float.class);
            emaValue10State = getRuntimeContext().getState(ema10State);
            ValueStateDescriptor<Float> ema100State = new ValueStateDescriptor<>(
                    "ema100", float.class);
            emaValue100State = getRuntimeContext().getState(ema100State);
        }

        /**
         * On each element added to the window check if it is an end of batch event
         * @param stockMeasurement
         * @param context
         * @param collector
         * @throws Exception
         */
        @Override
        public void processElement(StockMeasurement stockMeasurement, KeyedProcessFunction<String, StockMeasurement, EmaStream>.Context context, Collector<EmaStream> collector) throws Exception {
            float price = stockMeasurement.close;

            float ema10Alpha = 2F / (10F + 1F);
            float ema10Prev = emaValue10State.value() == null ? price : emaValue10State.value();
            float ema10 = ema10Alpha * price + (1F - ema10Alpha) * ema10Prev;
            emaValue10State.update(ema10);

            float ema100Alpha = 2F / (100F + 1F);
            float ema100Prev = emaValue100State.value() == null ? price : emaValue100State.value();
            float ema100 = ema100Alpha * price + (1F - ema100Alpha) * ema100Prev;
            emaValue100State.update(ema100);

            String advise;
            float minusPrev = ema10Prev - ema100Prev;
            float minusNew = ema10 - ema100;
            if (minusNew * minusPrev >= 0) {
              advise = "Stay";
            } else if (minusNew > minusPrev) {
              advise = "Buy";
            } else {
              advise = "Sell";
            }
            
            EmaStream emaStream = new EmaStream(stockMeasurement.symbol, context.timerService().currentProcessingTime(), emaValue10State.value(), emaValue100State.value(), advise);
            collector.collect(emaStream);
        }

    }

    public static class EmaStream {
        public String symbol;
        public long timestamp;
        public float ema10;
        public float ema100;
        public String advise;
        public EmaStream(String symbol, long timestamp, float ema10, float ema100, String advise) {
            this.symbol = symbol;
            this.timestamp = timestamp;
            this.ema10 = ema10;
            this.ema100 = ema100;
            this.advise = advise;
        }
    }
    
    public static class EmaStreamSerializationSchema implements KafkaRecordSerializationSchema<EmaStream> {
        private transient Gson gson;

        @Override
        public ProducerRecord<byte[], byte[]> serialize(EmaStream element, KafkaSinkContext context, @Nullable Long timestamp) {
            if (gson == null) {
                gson = new Gson();
            }
            String json = gson.toJson(element);
            return new ProducerRecord<>("ema", json.getBytes(StandardCharsets.UTF_8));
        }
    }

}