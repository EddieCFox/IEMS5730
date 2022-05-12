import os
import random
import time
from datetime import datetime

# modify to convert the ts to seconds  
def convert_to_seconds(ts): 
    intermediate = datetime.strptime(ts, "%Y-%M-%d %H:%m:%S" ) #Year-Month-Day Hour:Minute:Second
    seconds = time.mktime(intermediate.timetuple())
    return seconds

# modify this function, the sleep time should based on the time in the data
def twitterSleep(timeGap):

    time.sleep(timeGap)

def main():
    last_ts = None
    with open('/home/s1155160788/Assignment4/bitcoin_twitter.txt') as f:
        for line in f:

            # split the text and timestamp
            parts = line.rstrip().split(',')
            text = ' '.join(parts[:-1])
            ts = parts[-1]

            ts = convert_to_seconds(ts)  

            cmd = 'echo "' + text + '" | /usr/hdp/2.6.5.0-292/kafka/bin/kafka-console-producer.sh --broker-list dicvmd7.ie.cuhk.edu.hk:6667 --topic 1155160788-test'
            
            os.system(cmd)
            
            if last_ts is not None:
               timeGap = ts - last_ts
               if timeGap != 0:
                   twitterSleep(timeGap)

            last_ts = ts
            
if __name__ == '__main__':
    main()