#!/usr/bin/python
from operator import itemgetter
import sys

# We first initialize the variables as None.

current_user = None
user1 = None
user2 = None
pair_count = None
count1 = None
count2 = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # Parse the input we got from mapper.
    # Inputs are tab separated
    user1, user2, pair_count, count1, count2  = line.split('\t')

    # Convert variables to float. (currently string) to float

    try:
        pair_count = float(pair_count)
    except ValueError:
        # Was not a number, so silently ignore/discard the line
        continue

    try:
        count1 = float(count1)
    except ValueError:
        # Was not a number, so silently ignore/discard the line
        continue

    try:
        count2 = float(count2)
    except ValueError:
        # Was not a number, so silently ignore/discard the line
        continue
    
    # Calculate the similarity.
    # The equation for similarity is (Number of movies both pairs have watched) / (User A movies + User B movies - number of movies both pairs have watched)
    # The numerator is pair_count. 
    # The denominator is count1 + count2 - pair_count
    # Print user pair and all three counts (pair and inidividual)

    denominator = count1 + count2 - pair_count
    similarity = pair_count / denominator

    # Print the users and similarity. 
    # We print twice so that each user can have a copy of the similarity of the pair under their primary key (when sorting)

    print "%s\t%s\t%.3f" % (user1, user2, similarity)
    print "%s\t%s\t%.3f" % (user2, user1, similarity)