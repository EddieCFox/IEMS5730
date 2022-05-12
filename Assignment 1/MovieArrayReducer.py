#!/usr/bin/python
from operator import itemgetter
import sys

# input comes from STDIN (standard input)

# Here, I mirror the functionality from the previous reducer, where
# movie is the new user and user is the new count. The difference is 
# that user is not a number, so we don't need to do conversion from string
# to integer. 

# For current users, we create an empty array to hold all the users for a 
# particular movie for printing later.

# I print with space because of restrictions in Python 2 with printing.

current_movie = None
current_users = [] 
movie = None

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # Parse the input we got from mapper
    # This will put the movie of this line in movie and the user in user.

    movie, user = line.split('\t', 1)

    # Integer conversion code readded to convert user into integer 
    # for sorting. 

    try:
        user = int(user)
    except ValueError:
        # User was not a number, so silently ignore/discard the line
        continue

    # This is the equivalent of the count based code, but we 
    # are appending user to an array. 
    if current_movie == movie:
        current_users.append(user)
    
    else:
        if current_movie:
            # Write result to STDOUT when movie moves to the next key
            # First we print the user and a tab, before using a for loop
            # to iterate over the list and print the list one by one.

            # sort the array of users numerically in ascending order.

            current_users.sort()

            # prints movie without newline, separated by space
            print '%s' % (current_movie),

            #print the list of current users, separated by spaces
            for X in current_users:
                print X,
            print

            # clear the current users list for the next key.
            del current_users[:]

        # add the first user of the next key.
        current_users.append(user)

        # set current_mmovie equal to the next key.
        current_movie = movie
# Print the last key if needed. 
if current_movie == movie:
    # sort the array of users numerically in ascending order.
    current_users.sort()

    # Print the line
    print '%s' % (current_movie),
    for X in current_users:
        print X,
    print