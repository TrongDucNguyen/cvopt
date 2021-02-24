-- fixed table
CREATE TABLE IF NOT EXISTS customer_demographics
(
    cd_demo_sk                bigint,
    cd_gender                 string,
    cd_marital_status         string,
    cd_education_status       string,
    cd_purchase_estimate      int,
    cd_credit_rating          string,
    cd_dep_count              int,
    cd_dep_employed_count     int,
    cd_dep_college_count      int 
)
row format delimited fields terminated by '|'; 
load data local inpath '${hiveconf:LOCATION}/customer_demographics.dat' overwrite into table customer_demographics;


-- fixed table
CREATE TABLE IF NOT EXISTS date_dim
(
    d_date_sk                 bigint,
    d_date_id                 string,
    d_date                    string,
    d_month_seq               int,
    d_week_seq                int,
    d_quarter_seq             int,
    d_year                    int,
    d_dow                     int,
    d_moy                     int,
    d_dom                     int,
    d_qoy                     int,
    d_fy_year                 int,
    d_fy_quarter_seq          int,
    d_fy_week_seq             int,
    d_day_name                string,
    d_quarter_name            string,
    d_holiday                 string,
    d_weekend                 string,
    d_following_holiday       string,
    d_first_dom               int,
    d_last_dom                int,
    d_same_day_ly             int,
    d_same_day_lq             int,
    d_current_day             string,
    d_current_week            string,
    d_current_month           string,
    d_current_quarter         string,
    d_current_year            string 
)
row format delimited fields terminated by '|'; 
load data local inpath '${hiveconf:LOCATION}/date_dim.dat' overwrite into table date_dim;


-- fixed table
CREATE TABLE IF NOT EXISTS ship_mode(
      sm_ship_mode_sk           bigint               
,     sm_ship_mode_id           string              
,     sm_type                   string                      
,     sm_code                   string                      
,     sm_carrier                string                      
,     sm_contract               string                      
)
row format delimited fields terminated by '|'; 
load data local inpath '${hiveconf:LOCATION}/ship_mode.dat' overwrite into table ship_mode;


-- fixed table
CREATE TABLE IF NOT EXISTS time_dim
(
    t_time_sk                 bigint,
    t_time_id                 string,
    t_time                    int,
    t_hour                    int,
    t_minute                  int,
    t_second                  int,
    t_am_pm                   string,
    t_shift                   string,
    t_sub_shift               string,
    t_meal_time               string
)
row format delimited fields terminated by '|'; 
load data local inpath '${hiveconf:LOCATION}/time_dim.dat' overwrite into table time_dim;