from dwr import *
from dwr.non_partition.query7 import *

# create result table
hiveql.execute(query7_create_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(query7.format(
    table_name=RESULT_TABLE,                            
    store_sales=STORE_SALES,
))
