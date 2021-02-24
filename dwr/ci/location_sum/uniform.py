from dwr import *

INPUT_TABLE = "openaq_clean"
COUNT_BY_LOCATION = "count_by_location"
COUNT_BY_CITY = "count_by_city"
COUNT_BY_COUNTRY = "count_by_country"
VALUE_BY_COUNTRY = "value_by_country"
VALUE_BY_PARAMETER = "value_by_parameter"

sample_rates = [0.0001, 0.001, 0.01, 0.05, 0.1, 0.25]

for sample_rate in sample_rates:
    sample_table = uniform_sample(sample_rate, INPUT_TABLE, overwrite=True)

    # language=HQL
    drop_table(sample_table_name(COUNT_BY_LOCATION, UNIFORM, sample_rate))

    # language=HQL - (1)
    hiveql.execute("""
        CREATE TABLE IF NOT EXISTS {result_table} AS
        SELECT
            location,
            city,
            country,
            parameter,
            unit,
            COUNT(*) / {sample_rate} AS count,
            AVG(value) AS value,
            ((1.0 - {sample_rate})/({sample_rate}*{sample_rate})) * COUNT(*) AS count_var,
            (1 - {sample_rate})/POW(COUNT(*),2) * (SUM(value*value) - POW(SUM(value), 2)/COUNT(*)) AS value_var
        FROM {input_table}
        GROUP BY location, city, country, parameter, unit
    """.format(
        input_table=sample_table,
        result_table=sample_table_name(COUNT_BY_LOCATION, UNIFORM, sample_rate),
        sample_rate=sample_rate,
    ))

    # language=HQL
    drop_table(sample_table_name(COUNT_BY_CITY, UNIFORM, sample_rate))

    # language=HQL count by city  (2)
    hiveql.execute("""
        CREATE TABLE IF NOT EXISTS {result_table} AS
        SELECT
            city,
            country,
            parameter,
            unit,
            SUM(count) count,
            AVG(value) value,
            SUM(count_var) count_var,
            1/POW(COUNT(*),2) * (SUM(value_var) +
                POW(AVG(value),2)*(
                    SUM(POW(1-{p}, count)) - SUM(POW(1-{p},2 * count))
                )
            ) AS value_var
        FROM {input_table}
        GROUP BY city, country, parameter, unit
    """.format(
        input_table=sample_table_name(COUNT_BY_LOCATION, UNIFORM, sample_rate),
        result_table=sample_table_name(COUNT_BY_CITY, UNIFORM, sample_rate),
        p=sample_rate,
    ))

    # language=HQL
    drop_table(sample_table_name(COUNT_BY_COUNTRY, UNIFORM, sample_rate))

    # language=HQL count by country   (3)
    hiveql.execute("""
        CREATE TABLE IF NOT EXISTS {result_table} AS
        SELECT
            country,
            parameter,
            unit,
            SUM(count) count,
            SUM(count_var) count_var
        FROM {input_table}
        GROUP BY country, parameter, unit
    """.format(
        input_table=sample_table_name(COUNT_BY_CITY, UNIFORM, sample_rate),
        result_table=sample_table_name(COUNT_BY_COUNTRY, UNIFORM, sample_rate),
    ))

    # language=HQL
    drop_table(sample_table_name(VALUE_BY_COUNTRY, UNIFORM, sample_rate))

    # language=HQL count by country   (4)
    hiveql.execute("""
        CREATE TABLE IF NOT EXISTS {result_table} AS
        SELECT
            country,
            parameter,
            unit,
            SUM(value) value,
            SUM(value_var) as value_var
        FROM {input_table}
        GROUP BY country, parameter, unit
    """.format(
        input_table=sample_table_name(COUNT_BY_CITY, UNIFORM, sample_rate),
        result_table=sample_table_name(VALUE_BY_COUNTRY, UNIFORM, sample_rate),
    ))

    # language=HQL
    drop_table(sample_table_name(VALUE_BY_PARAMETER, UNIFORM, sample_rate))

    # language=HQL          (5)
    hiveql.execute("""
        CREATE TABLE IF NOT EXISTS {result_table} AS
        SELECT
            country,  
            SUM(value) value,
            SUM(value_var) value_var
        FROM {input_table}
        GROUP BY country
    """.format(
        input_table=sample_table_name(VALUE_BY_COUNTRY, UNIFORM, sample_rate),
        result_table=sample_table_name(VALUE_BY_PARAMETER, UNIFORM, sample_rate),
    ))

    # evaluate sample error
    sample_evaluate(
        table_name=COUNT_BY_COUNTRY,
        sample_type=UNIFORM,
        sample_rate=sample_rate,
        group_by_columns=['country', 'parameter', 'unit'],
        aggregation_columns=['count'],
    )

    # evaluate sample error
    sample_evaluate(
        table_name=VALUE_BY_PARAMETER,
        sample_type=UNIFORM,
        sample_rate=sample_rate,
        group_by_columns=['country'],
        aggregation_columns=['value'],
    )
