INPUT_TABLE = 'openaq_clean'
RESULT_TABLE1 = 'openaq_samg_q1'
RESULT_TABLE2 = 'openaq_samg_q2'

# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        {1} STRING,
        agg_value DOUBLE
    )
"""

# language=HQL
samg_q1 = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        unit,
        SUM(value)
    FROM {input_table}
    GROUP BY unit
"""

# language=HQL
samg_q1_sampled = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        unit,
        SUM(value / sample_rate)
    FROM {input_table}
    GROUP BY unit
"""

# language=HQL
samg_q2 = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        parameter,
        SUM(value)
    FROM {input_table}
    GROUP BY parameter
"""

# language=HQL
samg_q2 = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        parameter,
        SUM(value / sample_rate)
    FROM {input_table}
    GROUP BY parameter
"""
