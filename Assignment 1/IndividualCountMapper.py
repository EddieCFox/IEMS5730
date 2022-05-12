#!/usr/bin/python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    
    # remove leading and trailing whitespace
    line = line.strip()

    # split the line into words 
    words = line.split(",")
# we only take the first word, the user for this MapReduce job
    user = words[0]
# Print "User, 1"
    print '%s\t%s' % (user, 1)