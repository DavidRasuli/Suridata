This program matches employees as dwarves and giants,
whereas each person is both a dwarf and a giant, without self circulation, and maintaining uniqueness.



The main method has a given test with employees.

validate_and_clean() - A validation will take place to clean the data, making sure a unique Id could generated from the given employees.


generate_pairs() - The employees has a concatted unique index, which is used to make sure that an employee wouldn't be his own giant/dwarf, then shuffles the data, and create the pair.


multiprocessing_main() - will join the processes of generate_pairs.


Besides the pairs, the output data will print the process IDs.
