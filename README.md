# New York City Taxi fare prediction in Spark (Scala)
Predicting the fare of Taxi rides (with Spark-Scala)

This project is from the Kaggle New York City Taxi Fare Prediction https://www.kaggle.com/c/new-york-city-taxi-fare-prediction The goal is to build a predictive model with Spark able to predict the price a taxi ride based on several features such as the number of passengers, the departure and arrival coordinates and more. Here's the list of the features provided by Kaggle for this challenge.

- id: Unique identifier for each trip
- vendor_id: Code indicating the provider associated with the trip record
- pickup_datetime: Date and time when the meter was engaged
- dropoff_datetime: Date and time when the meter was disengaged
- passenger_count: Number of passengers in the vehicle (driver entered value)
- pickup_longitude: Longitude where the meter was engaged
- pickup_latitude: Latitude where the meter was engaged
- dropoff_longitude: Longitude where the meter was disengaged
- dropoff_latitude: Latitude where the meter was disengaged
- fare_amount: Fare of the ride
I also used external data in addition to the features provided my Kaggle to take into consideration the weather, the holidays/weekends.
