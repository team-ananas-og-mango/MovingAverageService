# Define the function to calculate the Exponential Moving Average
class CalculateEMA(KeyedProcessFunction):
    def __init__(self, days=38):
        self.state = None
        self.days = days
        self.stocks = []

    def open(self, runtime_context):
        state_desc = ValueStateDescriptor(f"Close", Types.DOUBLE())
        self.state = runtime_context.get_state(state_desc)

    def process_element(self, value, ctx):
        current = self.state.value()
        if current is None:
            current = Row(value.Time, 0, 0)

        # update the state's count
        current[1] = float(value.Close)

        # set the state's timestamp to the record's assigned event time timestamp
        current[2] = ctx.timestamp()

        self.state.update(current)

        # ctx.timer_service().register_event_time_timer(ctx.timestamp() + 10000)
        return current[0], ta.ema(current[1], length=1)

    """def on_timer(self, timestamp, ctx):
        # get the state for the key that scheduled the timer
        result = self.state.value()

        # check if this is an outdated timer or the latest timer
        if timestamp == result[2] + 10000:
            # emit the state on timeout
            yield result[0], ta.ema(result[1], length=2)"""
