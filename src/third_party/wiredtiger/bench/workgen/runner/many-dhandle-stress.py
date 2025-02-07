#/usr/bin/env python
# generated from ../wtperf/runners/many-dhandle-stress.wtperf
# The next lines are unneeded if this script is in the runner directory.
import sys
sys.path.append("/mnt/data0/ravi/work/wiredtiger/bench/workgen/runner")

from runner import *
from wiredtiger import *
from workgen import *

''' The original wtperf input file follows:
# This workload uses several tens of thousands of tables and the workload is evenly distributed
# among them. The workload creates, opens and drop tables, and it generates warning if the time
# taken is more than the configured max_idle_table_cycle.
conn_config="cache_size=10G,eviction=(threads_min=4,threads_max=4),file_manager=(close_idle_time=30),session_max=1000"
table_config="type=file"
table_count=15000
#max_idle_table_cycle=2
# Uncomment to fail instead of generating a warning
# max_idle_table_cycle_fatal=true
random_range=1500000000
pareto=10
range_partition=true
report_interval=5
checkpoint_threads=1
checkpoint_interval=30
populate_threads=1
#pre_load_data=true
# Uncomment to skip the populate phase, and use a database from a previous run as the baseline.
# create=false
icount=15000000
run_time=900
threads=((count=10,inserts=1,throttle=1000),(count=10,reads=1))
max_latency=1000
sample_interval=5
sample_rate=1
'''

context = Context()
conn_config = ""
conn_config += ",cache_size=10G,eviction=(threads_min=4,threads_max=4),file_manager=(close_idle_time=30),session_max=1000"   # explicitly added
conn = context.wiredtiger_open("create," + conn_config)
s = conn.open_session("")

wtperf_table_config = "key_format=S,value_format=S," +\
    "exclusive=true,allocation_size=4kb," +\
    "internal_page_max=64kb,leaf_page_max=4kb,split_pct=100,"
compress_table_config = ""
table_config = "type=file"
tables = []
table_count = 15000
for i in range(0, table_count):
    tname = "table:test" + str(i)
    table = Table(tname)
    s.create(tname, wtperf_table_config +\
             compress_table_config + table_config)
    table.options.key_size = 20
    table.options.value_size = 100
    table.options.range = 101000
    tables.append(table)

populate_threads = 1
icount = 15000000
random_range = 1500000000
pop_ops = Operation(Operation.OP_INSERT, tables[0])
pop_ops = op_populate_with_range(pop_ops, tables, icount, random_range, populate_threads)
pop_thread = Thread(pop_ops)
pop_workload = Workload(context, populate_threads * pop_thread)
pop_workload.run(conn)

ops = Operation(Operation.OP_INSERT, tables[0], Key(Key.KEYGEN_PARETO, 0, ParetoOptions(10)))
# Updated the range_partition to False, because workgen has some issues with range_partition true.
# Revert it back after WT-7332.
ops = op_multi_table(ops, tables, False)
thread0 = Thread(ops)
thread0.options.throttle=1000
thread0.options.throttle_burst=1.0

ops = Operation(Operation.OP_SEARCH, tables[0], Key(Key.KEYGEN_PARETO, 0, ParetoOptions(10)))
ops = op_multi_table(ops, tables, False)
thread1 = Thread(ops)

ops = Operation(Operation.OP_SLEEP, "30") + \
      Operation(Operation.OP_CHECKPOINT, "")
checkpoint_thread = Thread(ops)

workload = Workload(context, 10 * thread0 + 10 * thread1 + checkpoint_thread)
workload.options.report_interval=5
workload.options.run_time=900
workload.options.max_latency=1000
workload.options.sample_rate=1
workload.options.sample_interval_ms = 5000
# Uncomment to fail instead of generating a warning
# workload.options.max_idle_table_cycle_fatal = True
workload.options.max_idle_table_cycle = 2
workload.run(conn)

latency_filename = context.args.home + "/latency.out"
latency.workload_latency(workload, latency_filename)
conn.close()
