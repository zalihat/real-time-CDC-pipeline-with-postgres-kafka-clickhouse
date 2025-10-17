# print_stream.py
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# tiny test stream
ds = env.from_collection(
    collection=[(1, "alice"), (2, "bob"), (3, "carol")],
    type_info=Types.TUPLE([Types.INT(), Types.STRING()])
)

ds.print()   # prints to taskmanager logs
env.execute("print-stream-test")
