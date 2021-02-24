from dwr import *
from dwr.non_partition.openaq_sasg_multiple import *

# create result table
hiveql.execute(create_result_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(query1.format(
    result_table=RESULT_TABLE,
    input_table=INPUT_TABLE,
))