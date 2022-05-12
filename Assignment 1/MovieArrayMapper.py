#!/usr/bin/python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # split the line into words 
    words = line.split(",")

    # we create two variables here, user and movies.
    # The input is in the format User, Movie

    user = words[0]
    movie = words[1]
    
    # For this mapper, we print the movie, a tab, and then the user. 
    print '%s\t%s' % (movie, user)