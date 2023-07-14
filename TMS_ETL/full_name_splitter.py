"""TMS_ETL - Split full name in first and last names."""

def split_name(full_name: str) -> tuple[str, str]:
	'''Gets a string with a full name by Mexican standards.
	And returns two strings for first_name, and last_name'''
	full_name = ' '.join(full_name.split()) # Remove duplicated spaces
	split_name_str = full_name.split(' ')

	if len(split_name_str) == 2:
		last_name = split_name_str[0]
		first_name = split_name_str[1]
		return first_name, last_name

	parts = []
	current_composite_word = ''

	for current_word in split_name_str:
		if current_word in ('DE', 'DEL', 'SAN', 'LA', 'LOS',
		    'LAS', 'SANTA', 'COSS', 'Y'):
			current_composite_word += current_word + ' '
		else:
			# We found a normal word, stop the composite
			parts.append(current_composite_word + current_word)
			current_composite_word = ''

	if len(parts) == 3:
		first_name = parts[2]
	if len(parts) == 4:
		first_name = f'{parts[2]} {parts[3]}'
	if len(parts) >= 5:
		first_name = f'{parts[2]} {parts[3]} {parts[4]}'

	last_name = f'{parts[0]} {parts[1]}'
	return first_name, last_name

# Test 1
FULL_NAME = 'CHAVEZ MARTINEZ MARTHA'
EXPECTED_FIRST_NAME = 'MARTHA'
EXPECTED_LAST_NAME = 'CHAVEZ MARTINEZ'

actual_first_name, actual_last_name = split_name(FULL_NAME)
assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 2
FULL_NAME = 'MURILLO IBARRA BRENDA MARIANA'
EXPECTED_FIRST_NAME = 'BRENDA MARIANA'
EXPECTED_LAST_NAME = 'MURILLO IBARRA'

actual_first_name, actual_last_name = split_name(FULL_NAME)
assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 3
FULL_NAME = 'DE LA CRUZ VIZCARRA MIRIAM JACQUELINE'
EXPECTED_FIRST_NAME = 'MIRIAM JACQUELINE'
EXPECTED_LAST_NAME = 'DE LA CRUZ VIZCARRA'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 4
FULL_NAME = 'RODRIGUEZ DE LA ROSA MARIA DE LOURDES'
EXPECTED_FIRST_NAME = 'MARIA DE LOURDES'
EXPECTED_LAST_NAME = 'RODRIGUEZ DE LA ROSA'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 5
FULL_NAME = 'DE LA CRUZ MARTINEZ MARIA DE LOURDES'
EXPECTED_FIRST_NAME = 'MARIA DE LOURDES'
EXPECTED_LAST_NAME = 'DE LA CRUZ MARTINEZ'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 6
FULL_NAME = 'GARCIA DEL TORO JUAN DIEGO'
EXPECTED_FIRST_NAME = 'JUAN DIEGO'
EXPECTED_LAST_NAME = 'GARCIA DEL TORO'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 7 - Missing

# Test 8
FULL_NAME = 'SHIH-FEN  CHENG'
EXPECTED_FIRST_NAME = 'CHENG'
EXPECTED_LAST_NAME = 'SHIH-FEN'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 9
FULL_NAME = 'GAMBOA DE LEON BEATRIZ'
EXPECTED_FIRST_NAME = 'BEATRIZ'
EXPECTED_LAST_NAME = 'GAMBOA DE LEON'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 10
FULL_NAME = 'DE SANTIAGO PEÑA ERIKA MARGARITA'
EXPECTED_FIRST_NAME = 'ERIKA MARGARITA'
EXPECTED_LAST_NAME = 'DE SANTIAGO PEÑA'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 11
FULL_NAME = 'DE JESUS MACARIO ANA MARIA'
EXPECTED_FIRST_NAME = 'ANA MARIA'
EXPECTED_LAST_NAME = 'DE JESUS MACARIO'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 12
FULL_NAME = 'CASTAÑEDA Y ZARAGOZA SONIA GIOVANA'
EXPECTED_FIRST_NAME = 'SONIA GIOVANA'
EXPECTED_LAST_NAME = 'CASTAÑEDA Y ZARAGOZA'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 13
FULL_NAME = 'DE LA O RAMIREZ DANIELA DE LA PAZ'
EXPECTED_FIRST_NAME = 'DANIELA DE LA PAZ'
EXPECTED_LAST_NAME = 'DE LA O RAMIREZ'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 14
FULL_NAME = 'DE LOS REYES ESPARZA ANDRU DONOVAN DAVID'
EXPECTED_FIRST_NAME = 'ANDRU DONOVAN DAVID'
EXPECTED_LAST_NAME = 'DE LOS REYES ESPARZA'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 15
FULL_NAME = 'ZUÑIGA DE LOS SANTOS JAVIER ADRIAN'
EXPECTED_FIRST_NAME = 'JAVIER ADRIAN'
EXPECTED_LAST_NAME = 'ZUÑIGA DE LOS SANTOS'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 16
FULL_NAME = 'SANTA ROSA CERDA MARIA DE GUADALUPE'
EXPECTED_FIRST_NAME = 'MARIA DE GUADALUPE'
EXPECTED_LAST_NAME = 'SANTA ROSA CERDA'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME

# Test 17
FULL_NAME = 'RIOS COSS Y LEON VERONICA'
EXPECTED_FIRST_NAME = 'VERONICA'
EXPECTED_LAST_NAME = 'RIOS COSS Y LEON'

actual_first_name, actual_last_name = split_name(FULL_NAME)

assert actual_first_name == EXPECTED_FIRST_NAME
assert actual_last_name == EXPECTED_LAST_NAME
