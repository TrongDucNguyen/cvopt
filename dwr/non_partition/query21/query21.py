from dwr import *
from dwr.non_partition.query21 import *

# create result table
hiveql.execute(create_result_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(query21.format(
    table_name=RESULT_TABLE,
))
