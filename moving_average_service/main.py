import argparse
import json
import logging
import os
import time

from pyflink.common import Duration, Row, Types, WatermarkStrategy
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import (
    EmbeddedRocksDBStateBackend,
    ProcessWindowFunction,
    StreamExecutionEnvironment,
    TimeCharacteristic,
    TimeWindow,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    FlinkKafkaProducer,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TimeWindow, TumblingEventTimeWindows
from pyflink.table import StreamTableEnvironment

logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument("--kafka-brokers", default="localhost:9092", help="Kafka brokers")
parser.add_argument("--kafka-group-id", default="test_group", help="Kafka consumer group ID")
parser.add_argument("--topics-in", default="xcse", help="Input Kafka topic")
parser.add_argument("--topics-out", default="ema", help="Output Kafka topic")
args = parser.parse_args()

class EMACalculator(KeyedProcessFunction):
    def __init__(self) -> None:
        self.ema10_state = None
        self.ema100_state = None

    def open(self, runtime_context):
        # MapStateDescriptor
        ema10_state = ValueStateDescriptor(f"ema10_state", Types.DOUBLE())
        ema100_state = ValueStateDescriptor(f"ema100_state", Types.DOUBLE())
        self.ema10_state = runtime_context.get_state(ema10_state)
        self.ema100_state = runtime_context.get_state(ema100_state)

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        price = float(value.Close)
        timestamp = ctx.timestamp()

        # Calculate the EMA(10)
        ema10_alpha = 2 / (10 + 1)
        ema10_prev = self.ema10_state.value() or price
        ema10 = ema10_alpha * price + (1 - ema10_alpha) * ema10_prev
        self.ema10_state.update(ema10)

        # Calculate the EMA(100)
        ema100_alpha = 2 / (100 + 1)
        ema100_prev = self.ema100_state.value() or price
        ema100 = ema100_alpha * price + (1 - ema100_alpha) * ema100_prev
        self.ema100_state.update(ema100)

        ema10_value = self.ema10_state.value()
        ema100_value = self.ema100_state.value()
        if ema10_value and ema100_value is not None:
            advise = "Sell"  # Bearish
            minus_prev = ema10_prev - ema100_prev
            minus_new = ema10_value - ema100_value
            if minus_new * minus_prev >= 0:
                advise = "Stay"
            elif minus_new > minus_prev:
                advise = "Buy"  # Bullish
            """result = {
                "symbol": value.Symbol,
                "timestamp": timestamp,
                "ema10": ema10_value,
                "ema100": ema100_value,
                "advise": advise,
            }"""
            result = Row(
                symbol=value.Symbol,
                timestamp=int(time.time()),
                ema10=ema10_value,
                ema100=ema100_value,
                advise=advise,
            )
            yield result

    def calculate_ema(self, price, current_ema, smoothing_factor):
        alpha = 2 / (1 + smoothing_factor)
        return price * alpha + current_ema * (1 - alpha)


class KafkaRowTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return int(value.Time)


env = StreamExecutionEnvironment.get_execution_environment()
path = os.path.dirname(os.path.abspath(__file__))
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

type_info = Types.ROW_NAMED(
    [
        "Close",
        "High",
        "Interest",
        "Low",
        "Open",
        "Time",
        "Volume",
        "Symbol",
    ],
    [
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.STRING(),
        Types.DOUBLE(),
        Types.STRING(),
    ],
)

deserialization_schema = JsonRowDeserializationSchema.builder().type_info(type_info=type_info).build()

kafka_consumer = (
    KafkaSource.builder()
    .set_bootstrap_servers(args.kafka_brokers)
    .set_group_id(args.kafka_group_id)
    .set_topics(args.topics_in)
    .set_value_only_deserializer(deserialization_schema)
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .build()
)

type_info_out = Types.ROW_NAMED(
    [
        "symbol",
        "timestamp",
        "ema10",
        "ema100",
        "advise",
    ],
    [
        Types.STRING(),
        Types.INT(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.STRING(),
    ],
)

serialization_schema = JsonRowSerializationSchema.Builder().with_type_info(type_info=type_info_out).build()

watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(2)).with_timestamp_assigner(
    KafkaRowTimestampAssigner()
)

stream = (
    env.from_source(
        kafka_consumer,
        # WatermarkStrategy.no_watermarks(),
        watermark_strategy,
        "KafkaSource",
    ).key_by(lambda x: x["Symbol"])
    # .window(TumblingEventTimeWindows.of(Time.seconds(30)))
    # .process(CountWindowProcessFunction(), Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.INT()]))
    .process(EMACalculator(), type_info_out)
)

# Execute the program
# env.execute("moving-average-job")

sink = (
    KafkaSink.builder()
    .set_bootstrap_servers(args.kafka_brokers)
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic(args.topics_out)
        .set_value_serialization_schema(serialization_schema)
        .build()
    )
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build()
)

kafka_producer = FlinkKafkaProducer(
    topic=args.topics_out,
    serialization_schema=serialization_schema,
    producer_config={
        "bootstrap.servers": args.kafka_brokers,
        "group.id": args.kafka_group_id,
    },
)

# stream = stream.map(lambda x: x.to_row(type_info_out))
stream.sink_to(sink)
# stream.add_sink(kafka_producer)

# Execute the program
env.execute("moving-average-job")