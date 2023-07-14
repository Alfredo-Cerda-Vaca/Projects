"""A test to split a batch of iterables in multiprocessing tasks."""
import time
import random
import itertools
from multiprocessing import Process

def split_iterable(iterable, groups_num):
	return [itertools.islice(iterable, i, None, groups_num) for i in range(groups_num)]

def process(name: str):
	random_time = random.randint(1, 5)
	print(f'Process name: {name} started')
	print(f'Waiting for {random_time} seconds')
	time.sleep(random_time)
	print(f'Process name: {name} finished')

if __name__ == '__main__':
	p_1 = Process(target=process, args=('1',))
	p_1.start()

	p_2 = Process(target=process, args=('2',))
	p_2.start()

	p_1.join()
	p_2.join()

	# Example usage
	my_list = [1, 2, 3, 4, 5, 6, 7, 8, 9]
	NUM_GROUPS = 3

	cycles = split_iterable(my_list, NUM_GROUPS)

	for group in cycles:
		for item in group:
			print(item)
		print()
