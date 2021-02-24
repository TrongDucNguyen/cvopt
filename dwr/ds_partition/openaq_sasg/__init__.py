
from datetime import datetime, timedelta

# ds list and sample rate parameters
start_date = datetime.strptime('2017-01-01', '%Y-%m-%d').date()
end_date = datetime.strptime('2017-12-31', '%Y-%m-%d').date()
day_count = (end_date - start_date).days + 1
ds_list = [d.strftime('%Y-%m-%d') for d in (start_date + timedelta(n) for n in range(day_count))]

sample_rates = [0.01, 0.05, 0.1, 0.25, 0.5]





