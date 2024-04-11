# san-jose-parcel-apn-webscrape

Webscrapes, lightly cleans, and returns property tax data for San Jose properties from the Santa Clara County Property Tax Website. 

## Input: A list of Santa Clara County parcel numbers (APNs), each of which uniquely identifies a property, must be provided for the webscrape to successfully run. 
Because the requested data size for this particular analysis was over 225,000 records, most consumer computers encounter computation and memory errors.
Consequently, the python code must be run on chunks of the entire APN list. Computation on eleven chunks were successfully performed from an ordinary personal computer.

## Output: More than 50 columns listing various property tax data across each APN.

## Running the code
This code will not just run for you out-of-the-box. You will need to download packages and prepare the environment for each run.

1. Run `run_apn_webscrape.py` to collect raw data. Ideally you'd run this from several different computers (not terminals).
2. Run `clean_data_by_chunk.py` for each of the chunks you want. This can be run concurrently in many terminals to speed up computation time.
3. Run `combine_cleaned_data.py` after finishing #1 and #2, as this will combine all the data chunks together into one csv.

## Brief Analysis Description
The objective of this analysis was to determine whether tax dollars for roads in San Jose were enough to properly maintain the roads.
The data un-ironically reveals that the tax dollars are insuffient to cover the cost of the roads, which is why many citizens express frustration with wear & tear like potholes.
The implications of this study is that to properly maintain the roads, more taxes would be required, but a rather more import implication is that *roads freely "gifted" to cities & counties from the federal government actually act more as a liability than an asset*.
Because so much of spending happens from governments, society does not fully observe the obscene cost of cars/roads.
Cars are generally a very expensive/inefficient transportation solution and rather than increase taxes to maintain roads, perhaps the transportation paradigm should be altered.
