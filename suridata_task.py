import random
import multiprocessing

def validate_and_clean(employees):
    unique_indices = set()
    cleaned_employees = []

    for employee in employees:
        index = (employee["name"], employee["department"], employee["age"])

        if index not in unique_indices:
            unique_indices.add(index)
            cleaned_employees.append(employee)

    return cleaned_employees

def generate_pairs(chunk, shared_pairs):
    random.shuffle(chunk)

    pairs = []

    for i, dwarf in enumerate(chunk):
        giant = list(reversed(chunk))[i]
        pairs.append((dwarf['name'], giant['name']))

    shared_pairs.extend(pairs)

def multiprocessing_main(employees_data, num_processes, shared_pairs):
    cleaned_employees = validate_and_clean(employees_data)
    chunk_size = len(cleaned_employees) // num_processes
    employee_chunks = [cleaned_employees[i:i + chunk_size] for i in range(0, len(cleaned_employees), chunk_size)]

    processes = []

    for chunk in employee_chunks:
        process = multiprocessing.Process(target=generate_pairs, args=(chunk, shared_pairs))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

def main():
    employees_data = [
        {"department": 'R&D', "name": 'emp1', "age": 46},
        {"department": 'Sales', "name": 'emp2', "age": 28},
        {"department": 'R&D', "name": 'emp3', "age": 33},
        {"department": 'R&D', "name": 'emp4', "age": 29},
    ]

    with multiprocessing.Manager() as manager:
        shared_pairs = manager.list()

        num_processes = 2

        multiprocessing_main(employees_data, num_processes, shared_pairs)

        # Shuffle the final pairs to ensure randomness
        final_pairs = list(shared_pairs)
        random.shuffle(final_pairs)

        print("Final Dwarf-Giant Pairs:", final_pairs)

if __name__ == "__main__":
    main()
