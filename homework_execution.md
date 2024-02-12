### Question 1:
What happens when we execute dbt build --vars '{'is_test_run':'true'}' You'll need to have completed the "Build the first dbt models" video.

**It applies a limit 100 only to our staging models**

### Question 2:
What is the distribution between service type filtering by year 2020 data only, as done in the videos? You will need to complete "Visualising the data" videos, either using google data studio or metabase.

**Yellow: 92.8%**
**Green: 7.2%**

!['distribution between service type filtering by year 2020'](/images/service_type_distribution_2020.png)

### Question 3:
What is the code that our CI job will run?

**The code from a development branch requesting a merge to main**

### Question 4:
What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?
Create a staging model for the fhv data, similar to the ones made for yellow and green data. Add an additional filter for keeping only records with pickup time in year 2019. Do not add a deduplication step. Run this models without limits (is_test_run: false).

Create a core model similar to fact trips, but selecting from stg_fhv_tripdata and joining with dim_zones. Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. Run the dbt model without limits (is_test_run: false).

```console
SELECT COUNT(*) FROM `de-zoomcamp.trips_data_prod.fact_fhv_trips`;
// 22998722
```
**22998722**

### Question 5:
What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, including the fact_fhv_trips data.

```console
SELECT COUNT(*) FROM `de-zoomcamp.trips_data_prod.fact_trips` WHERE service_type = 'Yellow' and (extract(year from pickup_datetime),extract(month from pickup_datetime))=(2019,7);
// 3243346
SELECT COUNT(*) FROM `de-zoomcamp.trips_data_prod.fact_trips` WHERE service_type = 'Green' and (extract(year from pickup_datetime),extract(month from pickup_datetime))=(2019,7);
// 397669
SELECT COUNT(*) FROM `de-zoomcamp.trips_data_prod.fact_fhv_trips` WHERE (extract(year from pickup_datetime),extract(month from pickup_datetime))=(2019,7);
// 290680
```
**Yellow**

!['Yellow/Green Taxi - FHV trips per month'](/images/yellow_green_taxi_fhv_trips_per_month.png)

