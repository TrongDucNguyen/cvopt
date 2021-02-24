wait_for_users = WaitOperator(
    table='Users',
    partition='ds=2018-12-05'
)
wait_for_cities = WaitOperator(
    table='Cities',
    partition='ds=2018-12-05'
)
wait_for_countries = WaitOperator(
    table='Countries',
    partition='ds=2018-12-05'
)
sample_users = UniformSampleOperator(
    dep_list=[wait_for_users],
    sample_rate=0.01,
)
by_city = InsertOperator(
    dep_list=[wait_for_users, wait_for_cities],
    query="""
    INSERT INTO ByCity
    SELECT 
      city_id, 
      country_id, 
      sum(num_actions) / sample_rate as num_actions, 
      count(*) / sample_rate as num_users,
      sum(num_actions * num_actions) as num_actions_square, 
      ds
    FROM Users_sampled Users, Cities
    WHERE Users.ds='2018-12-05' and Cities.ds='2018-12-05'
      and Users.city_id=Cities.city_id and Cities.population > 100000
    GROUP BY city_id, country_id, ds
    HAVING num_actions > 10000
  """
)
by_country = InsertOperator(
    dep_list=[by_city, wait_for_countries],
    query="""
    INSERT INTO ByCountry
    SELECT 
      country, 
      sum(num_actions) as num_actions, 
      sum(num_users) as num_users,
      sum(num_actions_square) as num_actions_square,
      ds
    FROM ByCity, Countries
    WHERE ByCity.ds='2018-12-05' and Countries.ds='2018-12-05'
      and ByCity.country_id=Countries.country_id
    GROUP BY country_id, ds
  """
)
