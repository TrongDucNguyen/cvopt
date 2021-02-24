from dwr import *
from dwr.ds_partition.query26 import *

# create result table
hiveql.execute(query26_create_table.format('query26'))

for DS in ds_list:
    logging.info("DS: {0} - Full query".format(DS))
    hiveql.execute(query26.format(
        ds=DS,
        query26_table='query26',
        catalog_sales_table=CATALOG_SALES,
    ))
