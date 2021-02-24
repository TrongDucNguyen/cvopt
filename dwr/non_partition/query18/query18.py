from dwr import *
from dwr.non_partition.query18 import *

# create result table
hiveql.execute(create_result_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(query18.format(
    table_name=RESULT_TABLE,
    catalog_sales=CATALOG_SALES,
))
