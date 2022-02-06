from pyspark import SparkConf, SparkContext

# Initialization
conf = SparkConf().setMaster("local").setAppName("[Part 2] Spark RDD")
sc = SparkContext(conf=conf)

# Loading dataset TEXTFILE
data = sc.textFile("datasets/vaccinations.csv")

# Loading data from list PARALLELIZE
data_list = sc.parallelize([("France",5),("Espagne",3),("Angleterre",8)])
data_list.first()

# Show some data SAMPLE
def show_some_data(data,nb_samples=5):
    print(data.sample(withReplacement=False,fraction=nb_samples/data.count(),seed=0).collect())
## Sample of what is loaded from textFile
show_some_data(data)

# Show first data FIRST
def show_first_data(data):
    print(data.first())
## First row of the textFile
show_first_data(data)

# Show first rows of data TAKE
def show_nfirst_datas(data,nb_samples=5):
    print(data.take(nb_samples))
## 5 first rows of the textFile
show_nfirst_datas(data)

# Parse the dataset MAP
def parse_data(data):
    line = data.split(",")
    country_name = line[0]
    date = line[2]
    total_vacc = float(line[3] if line[3] != "" else -1)
    percent_vacc = float(line[4] if line[4] != "" else -1)
    nb_new_vacc_per_day = float(line[6] if line[6] != "" else -1)
    nb_fully_vacc = float(line[10] if line[10] != "" else -1)
    return (country_name,date,total_vacc,percent_vacc,nb_new_vacc_per_day,nb_fully_vacc)
data_vaccination_countries = data.map(parse_data)
show_first_data(data_vaccination_countries)

# Extract number of vaccinations in each country MAP
data_nb_fully_vacc = data_vaccination_countries.map(lambda x:(x[0],x[1],x[5]))
show_first_data(data_nb_fully_vacc)

# Same as MAP, not working because of the format MAPVALUES
data_nb_fully_vacc2 = data_vaccination_countries.mapValues(lambda x: (x[0],x[4]))
show_first_data(data_nb_fully_vacc2)
# Same as MAP, corrected MAPVALUES
data_nb_fully_vacc3 = data_vaccination_countries.map(lambda x: (x[0],(x[1],x[2],x[3],x[4],x[5]))).mapValues(lambda x: (x[0],x[4]))
show_first_data(data_nb_fully_vacc3)

# Filter datas where nb_fully_vacc is not known (ie = -1) FILTER
filtered_data = data_nb_fully_vacc.filter(lambda x : x[2] != -1.0)
show_nfirst_datas(filtered_data)

# Number of datas in total COUNT
nb_datas_total = data_nb_fully_vacc.count()
nb_datas_not_empty = filtered_data.count()
print(f"{nb_datas_not_empty}/{nb_datas_total}")

# Number of datas per country COUNTBYKEY
count_per_country = filtered_data.countByKey()
for key,val in count_per_country.items():
    print(key,val)

# Count per date COUNTBYVALUE
data_per_date = filtered_data.map(lambda x: x[1])
count_date = data_per_date.countByValue()
for key,val in sorted(count_date.items(),key=lambda x:x[1]):
    print(key,val)

# Visualize datas for key France LOOKUP
dates_France = sorted(filtered_data.lookup("France"))
print(f"Nb dates for France : {len(dates_France)}, latest date = {dates_France[-1]}")

# Visualize dates per country GROUPBYKEY
countries_and_dates = filtered_data.map(lambda x: (x[0],x[1]))
show_first_data(countries_and_dates.groupByKey())
show_first_data(countries_and_dates.groupByKey().mapValues(list))
show_first_data(countries_and_dates.groupByKey().mapValues(sorted))
show_first_data(countries_and_dates.groupByKey().mapValues(set))

# Visualize countries per date GROUPBY
show_first_data(countries_and_dates.groupBy(lambda x:x[1]).mapValues(list))

# Add a counter to the keys MAP
counting_datas = filtered_data.map(lambda x: (x[0],1))
show_nfirst_datas(counting_datas)

# Summing the counters REDUCEBYKEY
count_countries = counting_datas.reduceByKey(lambda x,y : x+y)
show_nfirst_datas(count_countries)
## Aggregate results and display COLLECT
results = count_countries.collect()
for r in results:
    print("{} : {} days of data".format(*r))
## Find min and max
results_sorted = sorted(results,key=lambda x : x[1])
print(f"{results_sorted[0][0]} ({results_sorted[0][1]}) - {results_sorted[-1][0]} ({results_sorted[-1][1]})")

# Create one row for each vaccination FLATMAP
show_nfirst_datas(filtered_data)

def split_datas(data):
    return [(data[0],data[1])]*int(data[2])
splitted = filtered_data.flatMap(split_datas)
show_nfirst_datas(splitted)



