import random
import multiprocessing
import logging
import pytest
from suridata import validate_and_clean, multiprocessing_main, calc_largest_integer_divisor

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(process)d] [%(threadName)s] %(message)s')


def test_odd_length_list():
    print("test_odd_length_list")
    employees_data = [
        {"department": 'R&D', "name": 'emp1', "age": 46},
        {"department": 'Sales', "name": 'emp2', "age": 28},
        {"department": 'R&D', "name": 'emp3', "age": 33},
        {"department": 'R&D', "name": 'emp4', "age": 29},
        {"department": 'R&D', "name": 'emp5', "age": 27},
        {"department": 'R&D', "name": 'emp6', "age": 46},
        {"department": 'Sales', "name": 'emp7', "age": 28},
    ]

    with multiprocessing.Manager() as manager:
        shared_pairs = manager.list()

        num_processes = calc_largest_integer_divisor(len(employees_data))
        multiprocessing_main(employees_data, num_processes, shared_pairs)

        # Shuffle the final pairs to ensure randomness
        final_pairs = list(shared_pairs)
        random.shuffle(final_pairs)

        assert len(final_pairs) == len(employees_data), "Length of pairs does not match the number of employees"

        # Create sets of dwarves and giants
        dwarves, giants = zip(*final_pairs)
        all_employees = set(emp["name"] for emp in employees_data)

        assert set(dwarves) == all_employees, "All employees must be present in the dwarves list"
        assert set(giants) == all_employees, "All employees must be present in the giants list"

        assert all(dwarf != giant for dwarf, giant in
                   final_pairs), "A pair of dwarf/giant must contain two different employees"


def test_empty_list():
    with multiprocessing.Manager() as manager:
        shared_pairs = manager.list()

        with pytest.raises(ValueError, match="Empty employee list"):
            validate_and_clean([])

        # Ensure that no pairs were generated
        assert len(shared_pairs) == 0, "Empty employee list"


def test_even_length_list():
    print("test_even_length_list")
    employees_data = [
        {"department": 'R&D', "name": 'emp1', "age": 46},
        {"department": 'Sales', "name": 'emp2', "age": 28},
        {"department": 'R&D', "name": 'emp3', "age": 33},
        {"department": 'R&D', "name": 'emp4', "age": 29},
        {"department": 'R&D', "name": 'emp5', "age": 27},
        {"department": 'Sales', "name": 'emp7', "age": 28},
    ]

    with multiprocessing.Manager() as manager:
        shared_pairs = manager.list()
        num_processes = calc_largest_integer_divisor(len(employees_data))
        multiprocessing_main(employees_data, num_processes, shared_pairs)

        assert len(shared_pairs) == len(employees_data), "Length of pairs does not match the number of employees"


def test_duplicate_employee_id_list():
    print("test_duplicate_employee_id_list")
    employees_data = [
        {"department": 'R&D', "name": 'emp1', "age": 30},
        {"department": 'Sales', "name": 'emp2', "age": 28},
        {"department": 'R&D', "name": 'emp1', "age": 30},  # Duplicate
        {"department": 'R&D', "name": 'emp4', "age": 29},
        {"department": 'R&D', "name": 'emp5', "age": 27},
        {"department": 'R&D', "name": 'emp6', "age": 46},
        {"department": 'Sales', "name": 'emp7', "age": 28},
    ]

    with pytest.raises(ValueError, match="Duplicate employee ID"):
        validate_and_clean(employees_data)


def test_malformed_data():
    malformed_data = [
        0, "str"
    ]

    with pytest.raises(ValueError, match="Malfunctioned JSON"):
        validate_and_clean(malformed_data)
