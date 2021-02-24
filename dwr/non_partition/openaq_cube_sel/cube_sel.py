from dwr import *
from dwr.non_partition.openaq_cube_sel import *

# create result table
hiveql.execute(create_result_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(cube_sql.format(
    result_table=RESULT_TABLE,
    input_table=INPUT_TABLE,
    predicate=PREDICATE,
))
