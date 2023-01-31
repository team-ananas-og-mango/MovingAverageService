"""
from pyflink.datastream import (
    EmbeddedRocksDBStateBackend,
    ProcessWindowFunction,
    StreamExecutionEnvironment,
    TimeCharacteristic,
    TimeWindow,
)
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

class EmaCalculator(KeyedProcessFunction):
    def __init__(self):
        self.prev_ema = None
        self.prev_ts = None
        self.alpha = 0.1

    def process_element(self, data, context):
        # Parse the incoming JSON data
        record = data
        ts = datetime.strptime(record["Time"], "%Y-%m-%d %H:%M:%S")
        price = float(record["Close"])

        # Calculate EMA
        if self.prev_ema is None:
            ema = price
        else:
            time_delta = ts - self.prev_ts
            span_seconds = time_delta.seconds + time_delta.microseconds / 1000000
            alpha = 2 / (len(str(span_seconds)) + 1)
            ema = alpha * price + (1 - alpha) * self.prev_ema

        self.prev_ts = ts
        self.prev_ema = ema

        # Emit the updated EMA
        context.output((record["stock"], ema))

    def open(self, runtime_context):
        # Set up the state descriptor with TTL
        state_ttl_config = (
            StateTtlConfig.builder(Time.mintes(5))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build()
        )
        state_desc = ValueStateDescriptor("prev_ema", Types.FLOAT())
        state_desc.enableTimeToLive(state_ttl_config)

        # Set up RocksDB state backend
        backend = RocksDBStateBackend.builder().setDbStoragePath("/path/to/rocksdb/storage").build()

        # Initialize the state and set the backend
        self.prev_ema = runtime_context.get_state(state_desc)
        runtime_context.get_operator_state_backend().set_state_backend(backend)
        runtime_context.get_keyed_state_backend().set_state_backend(backend)"""
