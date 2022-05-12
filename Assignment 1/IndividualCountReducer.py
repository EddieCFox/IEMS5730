#!/usr/bin/python
# Code mostly adapted from IERG 4300 Fall Tutorial 4 slides 17-21

from operator import itemgetter
import sys

# Changing word to user to be more accurate.

current_user = None
current_count = 0
user = None

# input comes from STDIN

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # Parse the input we got from mapper
    user, count = line.split('\t', 1)

    # Convert count (currently string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently ignore/discard the line
        continue

    # This If switch works because Hadoop sorts mapout by Key
    # before it is passed to the reducer

    if current_user == user:
        current_count += count
    else:
        if current_user:
            # Write result to STDOUT
            print '%s\t%s' % (current_user, current_count)
        current_count = count
        current_user = user
if current_user == user:
    print '%s\t%s' % (current_user, current_count)