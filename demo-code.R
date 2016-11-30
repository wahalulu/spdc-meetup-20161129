# using 1 master and 10 core m3.2xlarge nodes on Amazon AWS EMR
# using RStudio running on the master node
library(sparklyr)
library(dplyr)

sc <- spark_connect(master = "yarn-client")

# read the green taxi data csv
green <-
  spark_read_csv(sc = sc,
                 name = "green",
                 path = "s3://bigdatateaching/nyctaxi/green_tripdata_*.csv"
  )
#     user  system elapsed
#     0.176   0.040  99.325

# count the records
green %>% count

# read the yellow taxi data csv
yellow <-
  spark_read_csv(sc = sc,
                 name = "yellow",
                 path = "s3://bigdatateaching/nyctaxi/yellow_tripdata_*.csv"
  )
)
#user   system  elapsed
#2.064    0.464 1548.521

# count records
yellow %>% count

# aggregate by year - counts
  yellow_trip_by_year <- yellow %>%
  mutate(year = year(Trip_Pickup_DateTime)) %>%
  group_by(year) %>%
  summarize(n = n()) %>%
  collect() %>%
  mutate(cab = "yellow")
#user  system elapsed
#0.048   0.000  18.524

green_trip_by_year <- green %>%
  mutate(year = year(lpep_pickup_datetime)) %>%
  group_by(year) %>%
  summarize(n = n()) %>%
  collect() %>%
  mutate(cab = "green")

# Save as parquet files
spark_write_parquet(green, path = "s3://bigdatateaching/nyctaxi/green-parquet")
spark_write_parquet(yellow, path = "s3://bigdatateaching/nyctaxi/yellow-parquet")




