from dwr import *
from dwr.non_partition.openaq_mamg import *

# create result table
drop_table(RESULT_TABLE, DROP)
hiveql.execute(create_result_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(mamg.format(
    result_table=RESULT_TABLE,
    input_table=INPUT_TABLE,
))
