import time
import os
import csv

INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_masg_complex'

DROP = True

OUTPUT_FILE = 'openaq_masg_complex_time.csv'
OUTPUT_PATH = 'C:/Users/mshih/Downloads/'
OUTPUT = os.path.join(OUTPUT_PATH, OUTPUT_FILE)

sample_rates = [0.01]


# Time Export Function
# ------------ Usage -------------
# t1 = time.time()  # ---------------------------------------
# t2 = time.time()  # ---------------------------------------
# time_export(sample_rate, SAMPLE_TYPE, "Event", t2-t1)
# --------------------------------
def time_export(sample_rate, sample_type, time_type, elapsed_time):

    row = [sample_rate, sample_type, time_type, elapsed_time]

    with open(OUTPUT, 'a', newline='') as csvFile:
        w = csv.writer(csvFile)
        w.writerow(row)

    csvFile.close()


# language=HQL - create result table
create_result_table = """
    CREATE TABLE IF NOT EXISTS {0} (
        country STRING,
        avg_incre DOUBLE,
        cnt_incre DOUBLE
    )
"""

# language=HQL - Changing of bc overtime for each country.
masg = """
    WITH y16 AS (
        SELECT  country,
                AVG(value) AS avg_value,
                COUNT(*) AS cnt_value
        FROM {input_table}
        WHERE parameter = "o3" AND YEAR(local_time) = 2016
        GROUP BY country
    ), y17 AS (
        SELECT  country,
                AVG(value) AS avg_value,
                COUNT(*) AS cnt_value
        FROM {input_table}
        WHERE parameter = "o3" AND YEAR(local_time) = 2017
        GROUP BY country
    )
    INSERT OVERWRITE TABLE {result_table}
    SELECT  y17.country,
            y17.avg_value - y16.avg_value AS avg_incre,
            y17.cnt_value - y16.cnt_value AS cnt_incre
    FROM y16
    JOIN y17
    ON y16.country = y17.country
"""

# language=HQL
masg_sampled = """
    WITH y16 AS (
        SELECT  country,
                AVG(value) AS avg_value,
                SUM(1/sample_rate) AS cnt_value
        FROM {input_table}
        WHERE parameter = "o3" AND YEAR(local_time) = 2016
        GROUP BY country
    ), y17 AS (
        SELECT  country,
                AVG(value) AS avg_value,
                SUM(1/sample_rate) AS cnt_value
        FROM {input_table}
        WHERE parameter = "o3" AND YEAR(local_time) = 2017
        GROUP BY country
    )
    INSERT OVERWRITE TABLE {result_table}
    SELECT  y17.country,
            y17.avg_value - y16.avg_value AS avg_incre,
            y17.cnt_value - y16.cnt_value AS cnt_incre
    FROM y16
    JOIN y17
    ON y16.country = y17.country
"""

masg_ss = """
    WITH y16 AS (
        SELECT  country,
                SUM(value/sample_rate)/SUM(1/sample_rate) AS avg_value,
                SUM(1/sample_rate) AS cnt_value
        FROM {input_table}
        WHERE parameter = "o3" AND YEAR(local_time) = 2016
        GROUP BY country
    ), y17 AS (
        SELECT  country,
                AVG(value) AS avg_value,
                SUM(1/sample_rate) AS cnt_value
        FROM {input_table}
        WHERE parameter = "o3" AND YEAR(local_time) = 2017
        GROUP BY country
    )
    INSERT OVERWRITE TABLE {result_table}
    SELECT  y17.country,
            y17.avg_value - y16.avg_value AS avg_incre,
            y17.cnt_value - y16.cnt_value AS cnt_incre
    FROM y16
    JOIN y17
    ON y16.country = y17.country
"""