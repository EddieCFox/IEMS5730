#!/usr/bin/python

from operator import itemgetter
import sys

# This code is based on my IndividualCountReducer code.
# The main difference is the key is a pair of users 
# instead of just one user, and the separated is a space rather than a
# tab. 

# Changing current_user to current_users instead to reflect
# that the key is a pair of users.

current_users = None
current_count = 0
users = None

# input comes from STDIN

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # Parse the input we got from mapper.
    # None reflects default behavior, which is separation by whitespace.
    users, count = line.split(None, 1)

    # Convert count (currently string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently ignore/discard the line
        continue

    # This If switch works because Hadoop sorts mapout by Key
    # before it is passed to the reducer

    if current_users == users:
        current_count += count
    else:
        if current_users:
            # Write result to STDOUT
            print '%s\t%s' % (current_users, current_count)
        current_count = count
        current_users = users
if current_users == users:
    print '%s\t%s' % (current_users, current_count)