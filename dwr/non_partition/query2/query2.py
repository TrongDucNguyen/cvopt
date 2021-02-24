from dwr import *
from dwr.non_partition.query2 import *

# create result table
hiveql.execute(create_result_table.format(RESULT_TABLE))

logging.info("Full query")
hiveql.execute(query2.format(
    query2_table=RESULT_TABLE,
    web_sales_table=WEB_SALES,
    catalog_sales_table=CATALOG_SALES,
))
