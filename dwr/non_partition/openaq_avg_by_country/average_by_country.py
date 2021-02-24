from dwr import *
from dwr.non_partition.openaq_avg_by_country import *

# create result table
hiveql.execute(create_result_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(average_by_country_sql.format(
    result_table=RESULT_TABLE,
    input_table=INPUT_TABLE,
))
