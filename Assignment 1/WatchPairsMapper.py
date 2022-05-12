#!/usr/bin/python
from operator import itemgetter
from itertools import combinations # used for iterating all possible pairs
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # First I add every element of the line to a list, including the movie.
    # By default, split separates by space, which we want.
    users = line.split()

    # remove the first element, which is the movie.
    users.pop(0)

    # Use the combinations module to list every pair
    # Print every pair followed by 1 on its own line
    # Format: User A,User B (composite key) 1
    
    for pair in combinations(users, 2):
        user = ','.join(pair)
        print user,
        print '%s' % (1)