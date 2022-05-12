#!/usr/bin/python
from operator import itemgetter
import sys

IndividualCount = {}

# Open IndividualCount file and create dictionary of users and their individual counts.
with open('IndividualCountSmall') as f:
    for line in f.readlines():
        line = line.strip()

        # Append user and count from each file line to each list. 
        single_user, single_count = line.split('\t', 1)
        IndividualCount[single_user] = single_count

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # Parse the input we got from mapper.
    # Input comes in form of user_pair (tab) count
    users, pair_count = line.split('\t', 1)

    #Here we split up the comma divided pairs into individual users
    user1, user2 = users.split(',', 1)

    # Search through IndividualCount dictionary to find the
    # individual counts for each user.

    count1 = IndividualCount.get(user1)
    count2 = IndividualCount.get(user2)
    
    # Print User1 (tab) User2 (tab) PairCount (tab) User1Count (tab) User2Count (tab)
    print "%s\t%s\t%s\t%s\t%s" % (user1, user2, pair_count, count1, count2)