-- fixed table
CREATE TABLE IF NOT EXISTS customer_demographics
(
  cd_demo_sk                BIGINT,
  cd_gender                 STRING,
  cd_marital_status         STRING,
  cd_education_status       STRING,
  cd_purchase_estimate      INT,
  cd_credit_rating          STRING,
  cd_dep_count              INT,
  cd_dep_employed_count     INT,
  cd_dep_college_count      INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
LOAD DATA INPATH '${INPUT}/customer_demographics.dat' OVERWRITE INTO TABLE customer_demographics;


-- fixed table
CREATE TABLE IF NOT EXISTS date_dim
(
  d_date_sk                 BIGINT,
  d_date_id                 STRING,
  d_date                    STRING,
  d_month_seq               INT,
  d_week_seq                INT,
  d_quarter_seq             INT,
  d_year                    INT,
  d_dow                     INT,
  d_moy                     INT,
  d_dom                     INT,
  d_qoy                     INT,
  d_fy_year                 INT,
  d_fy_quarter_seq          INT,
  d_fy_week_seq             INT,
  d_day_name                STRING,
  d_quarter_name            STRING,
  d_holiday                 STRING,
  d_weekend                 STRING,
  d_following_holiday       STRING,
  d_first_dom               INT,
  d_last_dom                INT,
  d_same_day_ly             INT,
  d_same_day_lq             INT,
  d_current_day             STRING,
  d_current_week            STRING,
  d_current_month           STRING,
  d_current_quarter         STRING,
  d_current_year            STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
LOAD DATA INPATH '${INPUT}/date_dim.dat' OVERWRITE INTO TABLE date_dim;


-- fixed table
CREATE TABLE IF NOT EXISTS ship_mode(
  sm_ship_mode_sk           BIGINT,
  sm_ship_mode_id           STRING,
  sm_type                   STRING,
  sm_code                   STRING,
  sm_carrier                STRING,
  sm_contract               STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
LOAD DATA INPATH '${INPUT}/ship_mode.dat' OVERWRITE INTO TABLE ship_mode;


-- fixed table
CREATE TABLE IF NOT EXISTS time_dim
(
  t_time_sk                 BIGINT,
  t_time_id                 STRING,
  t_time                    INT,
  t_hour                    INT,
  t_minute                  INT,
  t_second                  INT,
  t_am_pm                   STRING,
  t_shift                   STRING,
  t_sub_shift               STRING,
  t_meal_time               STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';
LOAD DATA INPATH '${INPUT}/time_dim.dat' OVERWRITE INTO TABLE time_dim;