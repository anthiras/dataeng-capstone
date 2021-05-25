# Data Engineering Capstone Project

This repo contains my solution for the Data Engineering Capstone Project of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## 1. Scope

### 1.1 Data

The data I will be using for this projects is a dataset of U.S. flights in 2019 [1], including origin and destination airports, expected and actual departure/arrival times, etc. This will be combined with another dataset of airports [2] that includes basic information about airports, e.g. airport code, airport type, and location information.

The airport data is a JSON file containing 57,421 airports, while the flight data is one CSV file per month of the year 2019, for a total of approx. 6 million flights.

### 1.2 Goals

The goal of the project is to provide air traffic analysts with the ability to analyze flight data across airports.

Use cases:

* List the routes with the longest average flight delays.
* Compare delays across airport types (large/medium/small airports).
* Calculate statistics across airport elevation levels, for example delays due to weather conditions.

## 2. Data exploration

Please see `Data exploration.ipynb` where I take a look at a subset of the available data using pandas.

## 3. Choice of tools

The flight data may be too much for a single machine to handle, so a big data framework is needed. To support the analysis use cases the data should be transformed and stored into a format suitable for analytical queries, or a tool that provides such transformation on the fly as part of the query processing could be used.

Tools for consideration:

### 3.1 Apache Cassandra

With Cassandra it is possible to construct denormalized, distributed tables tailored for the analytical queries needed. Large amounts of data can be processed with high throughput, but tables must be modelled in advance to fit the queries, so there is a limitation on query flexibility. An ETL pipeline is needed to extract operational data, transform it into the Cassandra data model, and load it into Cassandra.

### 3.2 Amazon Redshift data warehouse

With a data warehouse like Amazon Redshift, a dimensional table model can be constructed to support the analytical queries. An ETL pipeline is needed to extract the operational data, transform it info the dimensional model, and load it into the warehouse. After this process is completed there is a lot of query flexibility, supporting filtering and aggregating the stored facts based on associated dimensions.

### 3.3 Apache Spark data lake

With a data lake architecture, a traditional separate ETL pipeline is not required. Instead, queries can be performed directly on original data files, and any transformation is performed as part of the process. In this case, the query and ETL can be considered integrated, and the data model is defined by the query. This offers a lot of flexibility of doing ad hoc queries.

Based on these considerations, I will choose Apache Spark for this project.

## 4 Data models and ETL

Since I will be transforming the data as part of the query with Apache Spark, I will end up with data models representing the query output.

The ETL pipeline and query will be a unified process following this structure:

1. Load flights and airports data.
2. Preprocess airports: Only include US airports with an international airport code (IATA code).
3. Preprocess flights: Calculate total delay as sum of each delay type. Add a boolean cancelled column, calculated from the numeric cancelled column.
4. Combine flights and airports data using the origin and/or destination airport code from the flight data to look up the airport in the airport data.
5. Filter, group, and aggregate data depending on use case.

For the three use cases mentioned above I end up with the following data models:

**Delayed routes**

Delayed routes will contain origin and destination airport (name from airports data), average delay, and total flights aggregated from the flight data. The airport code is in both datasets and should be used as the join condition.

| Column        | Type   | Description                           |
| ------------- | ------ | ------------------------------------- |
| origin_code   | string | IATA code of origin airport           |
| origin_name   | string | Name of origin airport                |
| dest_code     | string | IATA code of destination airport      |
| dest_name     | string | Name of destination airport           |
| avg_delay     | double | Average delay in minutes              |
| total_flights | int    | Total number of flights on this route |

**Airport type statistics**

Delays, number of total and cancelled flights are obtained from the flight data. Joining with the airports data on the origin airport code will provide the airport type, and the data can be aggregated by grouping on the airport type column. Cancelleration rate is a calculated value, comparing cancelled flights to total flights.

| Column                  | Type   | Description                                                        |
| ----------------------- | ------ | ------------------------------------------------------------------ |
| airport_type            | string | Type of airport, e.g. small_airport, medium_airport, large_airport |
| total_flights           | int    | Total number of flights for originating at this type of airport    |
| cancelled_flights       | int    | Number of cancelled flights originating at this type of airport    |
| avg_total_delay         | double | Average total delay                                                |
| avg_carrier_delay       | double | Average delay caused by the carrier                                |
| avg_weather_delay       | double | Average weather delay                                              |
| avg_nas_delay           | double | Delay caused by the National Airspace System                       |
| avg_security_delay      | double | Average security delay                                             |
| avg_late_aircraft_delay | double | Average late aircraft delay                                        |
| cancellation_rate       | double | Rate of cancelled flights, between 0 and 1                         |

**Elevation statistics**

Delays and number of total flights are obtained from the flight data. This will be grouped by the destination airport elevation level from the airport data, and average statistics will be calculated. The elevation in feet is grouped into thousands (0-999 feet, 1000-1999, 2000-2999 feet etc.).

| Column                  | Type   | Description                                                                             |
| ----------------------- | ------ | --------------------------------------------------------------------------------------- |
| elevation_level         | string | Elevation level in groups of 1000 feet, e.g. 0-999 feet, 1000-1999, 2000-2999 feet etc. |
| total_flights           | int    | Total number of flights                                                                 |
| avg_total_delay         | double | Average total delay                                                                     |
| avg_carrier_delay       | double | Average delay caused by the carrier                                                     |
| avg_weather_delay       | double | Average weather delay                                                                   |
| avg_nas_delay           | double | Delay caused by the National Airspace System                                            |
| avg_security_delay      | double | Average security delay                                                                  |
| avg_late_aircraft_delay | double | Average late aircraft delay                                                             |

## 5. Output

The output of the three queries above is included here:

### 5.1 Delayed routes

|origin_code|         origin_name|dest_code|           dest_name|        avg_delay|total_flights|
|-----------|--------------------|---------|--------------------|-----------------|-------------|
|        EGE|Eagle County Regi...|      IAD|Washington Dulles...|            804.0|           45|
|        VPS|Destin-Ft Walton ...|      SRQ|Sarasota Bradento...|            744.0|            1|
|        FAR|Hector Internatio...|      PHX|Phoenix Sky Harbo...|            535.5|           33|
|        PHX|Phoenix Sky Harbo...|      FAR|Hector Internatio...|            519.0|           33|
|        GEG|Spokane Internati...|      LAX|Los Angeles Inter...|            518.0|            4|
|        EGE|Eagle County Regi...|      SLC|Salt Lake City In...|448.6666666666667|           10|
|        ATL|Hartsfield Jackso...|      FAR|Hector Internatio...|            411.5|           23|
|        MYR|Myrtle Beach Inte...|      FWA|Fort Wayne Intern...|            378.0|           22|
|        SLC|Salt Lake City In...|      EGE|Eagle County Regi...|            354.0|           10|
|        LAS|McCarran Internat...|      OGG|     Kahului Airport|            350.0|           10|

The results show flights from Eagle County Regional Airport to Washington Dulles Airport are the most delayed, with more than 13 hours (804 minutes) on average.

### 5.2 Airport type statistics

|  airport_type|total_flights|cancelled_flights|  avg_total_delay| avg_carrier_delay| avg_weather_delay|     avg_nas_delay| avg_security_delay|avg_late_aircraft_delay|   cancellation_rate|
|--------------|-------------|-----------------|-----------------|------------------|------------------|------------------|-------------------|-----------------------|--------------------|
| small_airport|        24619|            693.0|88.56969188849298|28.158457346468246| 7.389436176902117|15.686019702368476|0.07356948228882834|      37.26220918046531|0.028148990617003128|
|medium_airport|       627544|          13160.0|81.23443624625317|24.968459380524173|5.8340634847436785|17.807249634924293| 0.1042195065713627|      32.52044423948966|0.020970641102456562|
| large_airport|      6700271|         120519.0|67.94612426407201|20.731326926252727| 3.647325058332984|16.565817700831005|0.09436866446108856|     26.907285914194215| 0.01798718290648244|

Here are statistics across three types of airports (small/medium/large). The large airports have by far the most flights, but a lower cancellation rate, and generally lower delays.

### 5.3 Elevation statistics

|elevation_level|total_flights|avg(carrier_delay)|avg(weather_delay)|    avg(nas_delay)| avg(security_delay)|avg(late_aircraft_delay)|  avg(total_delay)|
|---------------|-------------|------------------|------------------|------------------|--------------------|------------------------|------------------|
|              0|      5753972|20.713654777330696|3.7288540316897407| 17.65524472027098| 0.09540385096274069|       27.44024278796972| 69.63340016822387|
|           1000|       831358| 23.35260895531192| 4.600793340657234|11.743720000884766| 0.08734857589453583|      27.732380243163334| 67.51685111591179|
|           2000|       242806|18.858100558659217|2.5772737430167596|12.036871508379889| 0.12632402234636872|      25.972916201117318|59.571486033519555|
|           3000|        51881|25.178736993921913| 5.435974039353044| 8.003399608529927| 0.07252498197177294|      27.314206242917482| 66.00484186669414|
|           4000|       153564|24.944711432476524|4.9186984700850935| 8.929676822009611| 0.11965962700057317|      27.193509986332174| 66.10625633790397|
|           5000|       284194|20.838668162012098|3.6222536816752773|17.902925116287623| 0.08199886010553216|      26.061682999025575|  68.5075288191061|
|           6000|        24648|27.729016786570742| 6.356022874008485| 11.25548791735842| 0.05404906843755765|       29.18151632540122| 74.57609297177642|
|           7000|        10011| 40.27272727272727|  10.6087160262418|14.695407685098406|0.019212746016869727|       27.23008434864105|  92.8261480787254|

The statistics across airport elevation levels show higher weather delays for the airports over 6000 feet elevation. Also delays caused by the air carrier's operations seems higher at these high levels of elevation. Other types of delay do not seem to have a significant difference across elevation levels.

## 6. Final remarks

### 6.1 Current data scenario

In this project I processed two datasets containing 57,421 airports and 7,422,037 flights. For large datasets, Spark seems like a good option for doing these kinds of ad-hoc queries, for example for data exploration, since queries and transformations can be distributed and executed without and intermediate database/data warehouse.

### 6.2 Other scenarios

Let's consider how this data pipeline would work in a few other scenarios:

**If the data was increased by 100x:**
Scaling up to large amounts of data is the big advantage of using a framework like Spark. Since Spark can split the data and run distributed computation it should be possible to add more nodes and still run the same pipeline.

**If the pipeline was run daily at 7 AM:**
Running the pipeline daily using a scheduling system would certainly be possible. The results could be saved in a daily report. However, more control over the time interval would probably be needed. In this project I am simply querying all 2019 data. For a daily pipeline it would probably be more relevant to look at data from the previous day only, and only generate yearly reports once a year. Additionally, it would be a good idea to put a monitoring system in place to track the daily runs and alert if it fails.

**If the data needs to be accessed by 100+ people:**
It would be very inefficient to have 100+ executions of the same pipeline. Instead, the results should be saved into a report or database for distribution. With more users consuming the statistics it may also become relevant to have more query flexibility, i.e. options for various filters and aggregations. In this case a data warehouse like Amazon Redshift could be useful.

## 7. How to run

You will need an AWS access and secret access key to access the data in S3. Copy `config.cfg.example` to `config.cfg` and fill out the credentials.

The code runs in a Python 3 environment with the following packages installed:

```
jupyterlab
pandas
pyspark
s3fs
```

Additionally, Spark must be installed and the `SPARK_HOME` variable must point to the spark installation.

Run the `Data exploration.ipynb` using Jupyter notebook.

Run the ETL pipeline as a Python script:

```
python3 main.py
```

## References

[1] Bureau of Transporation Statistics: Reporting Carrier On-Time Performance, https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ

[2] Airport Codes dataset, OurAirports.com via datahub.io, https://datahub.io/core/airport-codes
