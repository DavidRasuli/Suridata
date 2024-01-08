import random
import multiprocessing
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(process)d] [%(threadName)s] %(message)s')


def validate_and_clean(employees):
    if not employees:
        raise ValueError("Empty employee list")

    unique_indices = set()
    cleaned_employees = []

    for employee in employees:
        if not isinstance(employee, dict):
            raise ValueError("Malfunctioned JSON")

        index = (employee.get("name"), employee.get("department"), employee.get("age"))

        if None in index:
            raise ValueError("Empty employees array")

        if index in unique_indices:
            raise ValueError("Duplicate employee ID")

        unique_indices.add(index)
        cleaned_employees.append(employee)

    return cleaned_employees


def generate_pairs(chunk, shared_pairs):
    process_id = multiprocessing.current_process().pid

    random.shuffle(chunk)

    pairs = []

    for i in range(len(chunk)):
        dwarf = chunk[i]
        giant = chunk[(i + 1) % len(chunk)]  # Ensure circular pairing

        pairs.append((dwarf['name'], giant['name']))

    shared_pairs.extend(pairs)

    logging.info(f"Process ID: {process_id}, Dwarf-Giant Pairs: {pairs}")


def multiprocessing_main(employees_data, num_processes, shared_pairs):
    cleaned_employees = validate_and_clean(employees_data)
    chunk_size = len(cleaned_employees) // num_processes
    employee_chunks = [cleaned_employees[i:i + chunk_size] for i in range(0, len(cleaned_employees), chunk_size)]

    processes = []

    for i, chunk in enumerate(employee_chunks):
        process = multiprocessing.Process(target=generate_pairs, args=(chunk, shared_pairs))
        process.start()
        processes.append(process)

        logging.info(f"Started Process {i + 1} with ID {process.pid}")

    for process in processes:
        process.join()


def calc_largest_integer_divisor(n):
    return next(d for d in range(n - 1, 0, -1) if n % d == 0)

