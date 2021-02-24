from dwr import *
from dwr.non_partition.trips_sasg_2 import *

# create result table
hiveql.execute(create_result_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(sasg.format(
    result_table=RESULT_TABLE,
    input_table=INPUT_TABLE,
))
