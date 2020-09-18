#!/bin/bash
set -e  # exit immediately

echo "Parallel:"
time (python pgsql_big_dedupe_example.py > /dev/null 2>&1)
echo "Parallel old:"
time (python pgsql_big_dedupe_example_old.py > /dev/null 2>&1)
echo "Serial:"
time (python pgsql_big_dedupe_example_serial.py > /dev/null 2>&1)
echo "Serial old:"
time (python pgsql_big_dedupe_example_serial_old.py > /dev/null 2>&1)
