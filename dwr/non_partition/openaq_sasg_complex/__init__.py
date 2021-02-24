import time
import os
import csv

INPUT_TABLE = 'openaq_clean'
RESULT_TABLE = 'openaq_sasg_complex'

DROP = True

OUTPUT_FILE = 'openaq_sasg_complex_time.csv'
OUTPUT_PATH = 'C:/Users/mshih/Downloads/'
OUTPUT = os.path.join(OUTPUT_PATH, OUTPUT_FILE)


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
        month STRING,
        year STRING,
        average DOUBLE
    )
"""

# language=HQL - average by country parameter, unit
sasg = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        month,
        year,
        AVG(value)        
    FROM 
    (
        SELECT 
            country,
            MONTH(local_time) AS month ,
            YEAR(local_time) AS year ,
            value
        FROM {input_table}
        WHERE parameter = 'co' 
    ) t
    GROUP BY country, month , year
"""

sasg_ss = """
    INSERT OVERWRITE TABLE {result_table}
    SELECT
        country,
        month,
        year,
        SUM(value/sample_rate) / SUM(1/sample_rate)        
    FROM 
    (
        SELECT 
            country,
            MONTH(local_time) AS month ,
            YEAR(local_time) AS year ,
            value,
            sample_rate
        FROM {input_table}
        WHERE parameter = 'co' 
    ) t
    GROUP BY country, month , year
"""
