"""system configuration process"""
from connection import retrieve_result_where


def process_services():
    """."""
    system_configurations = retrieve_result_where()
    data = []
    for row in range(0, int(len(system_configurations))):
        property_value = system_configurations[row][2]

        for row1 in range((0), int(len(system_configurations))):
            comparate_property = system_configurations[row1][0]
            if property_value in comparate_property:
                print(f"iguales {property_value}   {comparate_property}  {system_configurations[row][2]}")
                data.append()



process_services()
