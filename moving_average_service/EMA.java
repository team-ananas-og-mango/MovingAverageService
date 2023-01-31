package moving_average_service;

import java.sql.Time;
import java.time.Instant;
import java.util.Objects;
import java.util.stream.Collector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.omg.CORBA.Context;

public class EMA {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    FlinkKafkaConsumer<Tuple8<Double, Double, Double, Double, Double, String, Double, String>> kafkaConsumer = new FlinkKafkaConsumer<>(
        "xcse",
        new JSONKeyValueDeserializationSchema(true),
        FlinkKafkaConsumer.readCommittedOffsets().withBootstrapServers("localhost:9092"));

    env
        .addSource(kafkaConsumer)
        .assignTimestampsAndWatermarks(WatermarkStrategy
            .<Tuple8<Double, Double, Double, Double, Double, String, Double, String>>forMonotonousTimestamps()
            .withTimestampAssigner((Tuple8<Double, Double, Double, Double, Double, String, Double, String> value,
                long timestamp) -> (long) (value.f5 * 1000)))
        .keyBy(value -> value.f7)
        .window(TumblingEventTimeWindows.of(Time.seconds(30)))
        .process(new EMACalculatorProcessFunction())
        .print();

    env.execute("EMA Calculator Job");
  }

  public static class EMACalculatorProcessFunction extends
      KeyedProcessFunction<String, Tuple8<Double, Double, Double, Double, Double, String, Double, String>, Tuple5<String, Long, Double, Double, String>> {

    private transient Double ema10State;
    private transient Double ema100State;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
      ema10State = null;
      ema100State = null;
    }

    @Override
    public void processElement(Tuple8<Double, Double, Double, Double, Double, String, Double, String> value,
        Context ctx, Collector<Tuple5<String, Long, Double, Double, String>> out) throws Exception {
      double price = value.f0;
      long timestamp = ctx.timestamp();

      double ema10Alpha = 2 / (10 + 1);
      double ema10Prev = Objects.requireNonNullElse(ema10State, price);
      double ema10 = ema10Alpha * price + (1 - ema10Alpha) * ema10Prev;
      ema10State = ema10;

      double ema100Alpha = 2 / (100 + 1);
      double ema100Prev = Objects.requireNonNullElse(ema100State, price);
      double ema100 = ema100Alpha * price + (1 - ema100Alpha) * ema100Prev;
      ema100State = ema100;

      String advise;
      double minusPrev = ema10Prev - ema100Prev;
      double minusNew = ema10 - ema100;
      if (minusNew * minusPrev >= 0) {
        advise = "Stay";
      } else if (minusNew > minusPrev) {
        advise = "Buy";
      } else {
        advise = "Sell";
      }

      out.collect(new Tuple5<>(value.f7, Instant.now().getEpochSecond(), ema10, ema100, advise));
    }
  }
}