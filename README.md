# New York City Taxi fare prediction in Spark (Scala)
Predicting the fare of Taxi rides (with Spark-Scala)

This project is from the Kaggle New York City Taxi Fare Prediction https://www.kaggle.com/c/new-york-city-taxi-fare-prediction The goal is to build a predictive model with Spark able to predict the price a taxi ride based on several features such as the number of passengers, the departure and arrival coordinates and more. Here's the list of the features provided by Kaggle for this challenge.

id: a unique identifier for each trip
vendor_id:= a code indicating the provider associated with the trip record
pickup_datetime: date and time when the meter was engaged
dropoff_datetime: date and time when the meter was disengaged
passenger_count: the number of passengers in the vehicle (driver entered value)
pickup_longitude: the longitude where the meter was engaged
pickup_latitude: the latitude where the meter was engaged
dropoff_longitude:= the longitude where the meter was disengaged
dropoff_latitude: the latitude where the meter was disengaged
store_and_fwd_flag:= This flag indicates whether the trip record was held in vehicle memory before sending to the vendor because the vehicle did not have a connection to the server - Y=store and forward; N=not a store and forward trip
trip_duration: duration of the trip in seconds
fare_amount: fare of the ride
I also used external data in addition to the features provided my Kaggle to take into consideration the weather, the holidays/weekends.
