"""BOK"""
# Dev By: e-alfredo_cerda@usiglobal.com
from BOK.connection import new_employees_search, update_employees_courses

def validation_new_employees_or_update(new_employees: list[int], updated_employees: list[int]):
	"""Choose new employees or update employees."""
	if len(new_employees) > 0:
		new_employees_search(new_employees)

	if len(updated_employees) > 0:
		update_employees_courses(updated_employees)
