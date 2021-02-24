from dwr import *

INPUT_TABLE = "openaq_clean"
COUNT_BY_LOCATION = "count_by_location"
COUNT_BY_CITY = "count_by_city"
COUNT_BY_COUNTRY = "count_by_country"
VALUE_BY_COUNTRY = "value_by_country"
VALUE_BY_PARAMETER = "value_by_parameter"

# (1) -> (2) -> (3)
#           |-> (4) -> (5)


# language=HQL - count by location (1)
count_by_location = """
    CREATE TABLE IF NOT EXISTS {result_table} AS
    SELECT
        location,
        city,
        country,
        parameter, 
        unit, 
        COUNT(*) count,
        AVG(value) value
    FROM {input_table}
    GROUP BY location, city, country, parameter, unit
"""

# language=HQL - count by city  (2)
count_by_city = """
    CREATE TABLE IF NOT EXISTS {result_table} AS
    SELECT
        city,
        country,
        parameter, 
        unit, 
        SUM(count) count,
        AVG(value) value
    FROM {input_table}
    GROUP BY city, country, parameter, unit
"""

# language=HQL - count by country  (3)
count_by_country = """
    CREATE TABLE IF NOT EXISTS {result_table} AS
    SELECT
        country,
        parameter, 
        unit, 
        SUM(count) count
    FROM {input_table}
    GROUP BY country, parameter, unit
"""

# language=HQL  (4)
value_by_country = """
    CREATE TABLE IF NOT EXISTS {result_table} AS
    SELECT
        country,
        parameter, 
        unit, 
        SUM(value) value
    FROM {input_table}
    GROUP BY country, parameter, unit
"""

# language=HQL  (5)
value_by_country_2 = """
    CREATE TABLE IF NOT EXISTS {result_table} AS
    SELECT
        country,
        SUM(value) value
    FROM {input_table}
    GROUP BY country
"""

# count by location
drop_table(COUNT_BY_LOCATION)
hiveql.execute(count_by_location.format(
    input_table=INPUT_TABLE,
    result_table=COUNT_BY_LOCATION,
))

# count by city
drop_table(COUNT_BY_CITY)
hiveql.execute(count_by_city.format(
    input_table=COUNT_BY_LOCATION,
    result_table=COUNT_BY_CITY,
))

# count by country
drop_table(COUNT_BY_COUNTRY)
hiveql.execute(count_by_country.format(
    input_table=COUNT_BY_CITY,
    result_table=COUNT_BY_COUNTRY,
))

# value by country
drop_table(VALUE_BY_COUNTRY)
hiveql.execute(value_by_country.format(
    input_table=COUNT_BY_CITY,
    result_table=VALUE_BY_COUNTRY,
))

# value by parameter
drop_table(VALUE_BY_PARAMETER)
hiveql.execute(value_by_country_2.format(
    input_table=VALUE_BY_COUNTRY,
    result_table=VALUE_BY_PARAMETER,
))
