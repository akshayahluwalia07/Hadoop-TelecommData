# Hadoop-TelecommData
This project takes data sets form a Telecomm Industry and figures out callers who have had their call time more than 60 minutes and only if the call is an STD call (call which are made between two different states).
Sample data format - 
FromPhoneNumber|ToPhoneNumber|CallStartTime|CallEndTime|STDFlag
9665128505|8983006310|2015-03-01 09:08:10|2015-03-01 10:12:15|1
9665128505|8983006310|2015-03-01 07:08:10|2015-03-01 08:12:15|0
