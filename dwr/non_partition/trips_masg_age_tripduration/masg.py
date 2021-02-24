from dwr.CSRLSS import *
from dwr.non_partition.trips_masg_age_tripduration import *

# create result table
drop_table(RESULT_TABLE, DROP)
hiveql.execute(create_result_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(masg.format(
    result_table=RESULT_TABLE,
    input_table=INPUT_TABLE,
))
