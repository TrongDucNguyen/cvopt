from dwr import *
from dwr.ds_partition.query2 import *

# create result table
hiveql.execute(query2_create_table.format('query2'))

for ds in ds_list:
    logging.info("DS: {0} - Full query".format(ds))
    hiveql.execute(query2.format(
        ds=ds,
        query2_table='query2',
        web_sales_table=WEB_SALES,
        catalog_sales_table=CATALOG_SALES,
    ))
