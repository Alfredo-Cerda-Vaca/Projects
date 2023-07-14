"""TMS_ETL - Read view data."""
import time # To get execution time
from db_source import get_view_data

start_time = time.time() # Start the timer

employees_view_data = get_view_data()
print(f'Found {len(employees_view_data)} record(s)')

end_time = time.time()
execution_time = end_time - start_time
print('ETL process finished')
print(f'Process time: {execution_time:.2f} seconds')

# Example on 2023-06-21
# Found 17418 record(s)
# ETL process finished
# Process time: 14.82 seconds

# Example on 2023-06-29
# Found 17418 record(s)
# ETL process finished
# Process time: 34.59 seconds