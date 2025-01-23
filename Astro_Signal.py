from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

rdd = spark.sparkContext.textFile("assignment3/pulsar.dat")

# function to split the rdd into columns 
def create_rdd(line): 
    cols = line.split(" ")
    return (
        round(float(cols[0]), 0),      #ascension 
        round(float(cols[1]), 0),      #declination 
        round(float(cols[2]), 2),      #time 
        round(float(cols[3]), 0)       #frequency 
    )
split_rdd = rdd.map(create_rdd)

# grouping ascension, declination, and frequency
def group_values(col):
    asc, dec, freq = col[0], col[1], col[3]
    return (asc, dec, freq)

# creating a key-value pair: ((asc, dec, frequency), time)
def map_time(col):
    key = group_values(col)
    value = col[2]
    return (key, value)
mapped_rdd = split_rdd.map(map_time)

# the groups with different values will get mapped to a unique key
grouped_rdd = mapped_rdd.groupByKey()

# finding the average time period between blips
def blip_data(group):
    time_list = sorted(list(group[1]))     
    count = len(time_list)      

    intervals = list(map(lambda x: x[1] - x[0], zip(time_list[:-1], time_list[1:])))
    period = round(sum(intervals) / len(intervals), 2) if intervals else 0

    return (group[0], count, period)      
blip_data_rdd = grouped_rdd.map(blip_data)

# sorting the data based on the number of times a frequency occurs
sorted_blip_data_rdd = blip_data_rdd.sortBy(lambda x: x[1], ascending=False)
top_blips = sorted_blip_data_rdd.take(5)

print("\n")
print("The top 5 frequencies with the most blips are: ", *top_blips, sep="\n")

# looking at these results, it is evident that frequency 4448 contains the most blips, but we
# cannot say this is our final answer yet, since we used rounding, we are 10 standard deviations away.
# we need to come up with a solution that is lesser standard deviations away to get a more precise answer, 
# while still accounting for most of the data.

def create_rdd(line): 
    cols = line.split(" ")
    return (
        round(float(cols[0]), 2),      #ascension 
        round(float(cols[1]), 2),      #declination 
        round(float(cols[2]), 2),      #time 
        round(float(cols[3]), 2)       #frequency 
    )
split_rdd = rdd.map(create_rdd)

mapped_rdd = split_rdd.map(map_time)
grouped_rdd = mapped_rdd.groupByKey()
blip_data_rdd = grouped_rdd.mapValues(lambda times: (list(times), 1))


# looking into frequency 4448Hz,
max_blips_rdd = blip_data_rdd.filter(lambda x: 4447 <= x[0][2] <= 4449
                                                and 85 <= x[0][0] <= 87
                                                and 67 <= x[0][1] <= 69)
max_blip_count_f1 = max_blips_rdd.map(lambda x: x[1][1]).sum()
print("\n")
print("Looking into frequency 4448: ")
# we can use this as the baseline for the full range of blips present in 4448 MHz.

# finding the mean so we can move closer in standard deviations from the baseline,
freq = max_blips_rdd.flatMap(lambda x: [x[0][2]]) 
asc = max_blips_rdd.flatMap(lambda x: [x[0][0]]) 
dec = max_blips_rdd.flatMap(lambda x: [x[0][1]]) 

freq_list = freq.collect()
asc_list = asc.collect()
dec_list = dec.collect()

mean_freq = round(sum(freq_list) / len(freq_list), 1) if freq_list else 0
mean_asc = round(sum(asc_list) / len(asc_list), 1) if asc_list else 0
mean_dec = round(sum(dec_list) / len(dec_list), 1) if dec_list else 0

print(f"Mean frequency: {mean_freq}")
print(f"Mean ascension: {mean_asc}")
print(f"Mean declination: {mean_dec}")

print(f"When we are +/- 10 standard deviations away, we have {max_blip_count_f1} blips.")

# now, can test different ranges, but it is best practise to not go lesser than +/- 3 
# standard deviations away from the mean, since we need to account for as much data 
# as we can under the curve. 
sd5_blips_rdd = blip_data_rdd.filter(lambda x: 4447.5 <= x[0][2] <= 4448.5
                                                and 86.0 <= x[0][0] <= 87.0
                                                and 67.6 <= x[0][1] <= 68.6)
sd5_blip_count = sd5_blips_rdd.map(lambda x: x[1][1]).sum()
print(f"When we are +/- 5 standard deviations away, we have {sd5_blip_count} blips.")

sd3_blips_rdd = blip_data_rdd.filter(lambda x: 4447.7 <= x[0][2] <= 4448.3
                                                and 86.2 <= x[0][0] <= 86.8
                                                and 67.8 <= x[0][1] <= 68.4)
sd3_blip_count = sd3_blips_rdd.map(lambda x: x[1][1]).sum()
print(f"When we are +/- 3 standard deviations away, we have {sd3_blip_count} blips.")


# now, we can finalize our answer with certainity, since the outputs from all three ranges are 
# still higher than the second highest occuring frequency (3031 MHz), even when we are +/- 3 standard 
# deviations away from the mean, which is approximately 99.7% of the data.


# finding the time period
time_list_rdd = sd5_blips_rdd.flatMap(lambda x: [x[1][0]]) 
time_list_sorted = time_list_rdd.sortBy(lambda x: x[0])
time_list = time_list_sorted.collect()

min_time = time_list[0][0]  
max_time = time_list[-1][0] 

total_period = max_time - min_time

intervals = list(map(lambda x: x[1][0] - x[0][0], zip(time_list[:-1], time_list[1:])))
period = round(sum(intervals) / len(intervals), 2) if intervals else 0

asc_r1 = mean_asc - 0.30
asc_r2 = mean_asc + 0.30

dec_r1 = mean_dec - 0.30
dec_r2 = mean_dec + 0.4


# printing the final result
print("\n")
print("The final result:")
print(f"There are {sd5_blip_count} blips, ")
print(f"at a location ranging from ({asc_r1}, {dec_r1}) to ({asc_r2}, {dec_r2}), ")
print(f"with a frequency of {mean_freq} MHz, ")
print(f"and a period of {period} seconds, ")
print(f"for a total time period of {total_period} seconds.")

# output:
# There are 36 blips, 
# at a location ranging from (86.2, 67.8) to (86.8, 68.5), 
# with a frequency of 4448.0 MHz, 
# and a period of 1.1 seconds, 
# for a total time period of 38.5 seconds.