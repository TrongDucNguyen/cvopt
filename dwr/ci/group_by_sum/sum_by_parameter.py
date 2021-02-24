from dwr import *
from dwr.ci.group_by_sum import *

# create result table
hiveql.execute(create_result_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(sum_by_parameter_sql.format(
    result_table=RESULT_TABLE,
    input_table=INPUT_TABLE,
))
