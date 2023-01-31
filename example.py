from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble


class my_timestamp_assigner(TimestampAssigner):

    def __init__(self) -> None:
        super().__init__()

    def extract_timestamp(self, value, record_timestamp):
        return value[2]


def datastream_api_demo():
    # 1. create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    env.add_jars("file:////opt/flink/jobs/jar_files/flink-connector-jdbc-1.16.0.jar")
    env.add_jars("file:////opt/flink/jobs/jar_files/postgresql-42.5.0.jar")

    type_info = Types.ROW_NAMED(
        ["seller_id", "amount_usd", "sale_ts"],
        [Types.STRING(), Types.INT(), Types.LONG()])

    # 2. create source DataStream
    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(type_info=type_info).build()

    kafka_source = KafkaSource\
        .builder()\
        .set_bootstrap_servers('host.docker.internal:9092')\
        .set_group_id('test-group')\
        .set_topics('sales-usd')\
        .set_value_only_deserializer(deserialization_schema)\
        .set_starting_offsets(KafkaOffsetsInitializer.latest())\
        .build()

    my_timestamp_assigner_ = my_timestamp_assigner()

    ds = env.from_source(kafka_source, WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(my_timestamp_assigner_), "KafkaSource")

    # Flink Table from datastream
    inputTable = t_env.from_data_stream(ds, col("seller_id"), col("amount_usd"), col("sale_ts").rowtime)

    resultTable = inputTable\
        .window(Tumble.over(lit(60).seconds).on(col("sale_ts")).alias("w"))\
        .group_by(col("seller_id"), col("w"))\
        .select(col("seller_id"), col("w").start, col("w").end, col("amount_usd").sum.alias("1Minute_amount"))

    

    resultStream = t_env.to_changelog_stream(resultTable)

    type_info = Types.ROW([Types.STRING(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP(), Types.INT()])
    resultStream.add_sink(
        JdbcSink.sink(
            """insert into aggregated_sales (seller_id, window_start, window_end, aggregated_amount) values (?,?,?,?)""",
            type_info,
            JdbcConnectionOptions.JdbcConnectionOptionsBuilder()\
                .with_url("jdbc:postgresql://host.docker.internal:5432/acosom_assessment")\
                .with_driver_name('org.postgresql.Driver')\
                .with_user_name("postgres")\
                .with_password("secret")\
                .build(),
            JdbcExecutionOptions.builder()\
                .with_batch_interval_ms(1000)\
                .with_batch_size(20)\
                .with_max_retries(5)\
                .build()

        )
    )

    # resultStream.print()


    # 4. Produce back to kafka topic
    type_info = Types.ROW_NAMED(
        ["seller_id", "window_start", "window_end", "aggregated_amount"],
        [Types.STRING(), Types.SQL_TIMESTAMP(), Types.SQL_TIMESTAMP(), Types.INT()])

    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=type_info
    ).build()

    kafka_sink = KafkaSink\
        .builder()\
        .set_bootstrap_servers('host.docker.internal:9092')\
        .set_record_serializer(KafkaRecordSerializationSchema.builder()\
            .set_topic("aggregated-sales")\
            .set_value_serialization_schema(serialization_schema)\
            .build()
            )\
        .build()

    resultStream.sink_to(kafka_sink)

    # 5. execute the job
    env.execute('acosom_assessment')


if __name__ == '__main__':
    datastream_api_demo()


from org.apache.flink.streaming.api.functions.windowing import WindowFunction
from org.apache.flink.streaming.api.windowing.windows import TimeWindow
from org.apache.flink.util import Collector
from my_package.TemperatureEvent import TemperatureEvent

class Avg(WindowFunction[TemperatureEvent, TemperatureEvent, Long, TimeWindow]):

    def apply(self, key, window, values, out):
        sum = 0
        count = 0
        for value in values:
            sum += value.getTemperature()
            count += 1
        result = values.iterator().next()
        result.setTemperature(sum / count)
        out.collect(result)
