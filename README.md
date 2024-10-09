# TDT4225 - Assignment 2
**Group 18**  
**Fall 2024**

## Overview
This assignment involves working with the Geolife GPS Trajectory dataset using MySQL and Python to clean, integrate, and query large volumes of data.

## Dataset
We processed data from 182 users with over 24 million trackpoints. The cleaned data was structured into the following tables:
- **User**: (`id`, `has_labels`)
- **Activity**: (`id`, `user_id`, `transportation_mode`, `start_date_time`, `end_date_time`)
- **TrackPoint**: (`id`, `activity_id`, `lat`, `lon`, `altitude`, `date_time`)

## Tasks
### Part 1: Data Cleaning & Insertion
- Cleaned and inserted activities with fewer than 2500 trackpoints.
- Optimized batch insertion for trackpoints.

### Part 2: Queries
- Analyzed user activities, transportation modes, altitude gain, and distance covered using MySQL queries.

### Part 3: Report
https://docs.google.com/document/d/1vSWVOuNiyWG1eHXEds0gooZDQ0aLewjeAw74iN79AJ0/edit?usp=sharing
