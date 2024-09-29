#Part2 of assignment
from DbConnector import DbConnector
from tabulate import tabulate
import math
from haversine import haversine, Unit

connection = DbConnector()
db_connection = connection.db_connection
cursor = connection.cursor

#1. How many users, activities and trackpoints are there in the dataset (after it is inserted into the database)
def count_users_activities_trackpoints():
    cursor.execute("SELECT COUNT(*) FROM User")
    users = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM Activity")
    activities = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM TrackPoint")
    trackpoints = cursor.fetchone()[0]
    return users, activities, trackpoints


users, activities, trackpoints = count_users_activities_trackpoints()
print("Number of users: ", users)
print("Number of activities: ", activities)
print("Number of trackpoints: ", trackpoints)

#2. Find the average number of activities per user

def avg_activities_per_user():
    cursor.execute("SELECT COUNT(*) FROM Activity")
    activities = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM User")
    users = cursor.fetchone()[0]
    return activities/users

avg_activities = avg_activities_per_user()
print("Average number of activities per user: ", avg_activities)

#3. Find the top 20 users with the highest number of activities. 

def top_20_users_highest_activities():
    cursor.execute("SELECT User.id, COUNT(Activity.id) as activities FROM User JOIN Activity ON User.id = Activity.user_id GROUP BY User.id ORDER BY activities DESC LIMIT 20")
    users = cursor.fetchall()
    return users

top_20_users = top_20_users_highest_activities()
print(tabulate(top_20_users, headers=["User ID", "Number of Activities"]))

#4. Find all users who have taken a taxi.
def users_taken_taxi():
    cursor.execute("SELECT DISTINCT User.id FROM User JOIN Activity ON User.id = Activity.user_id WHERE Activity.transportation_mode = 'taxi'")
    users = cursor.fetchall()
    return users

users_taxi = users_taken_taxi()
print("Users who have taken a taxi: ", users_taxi)

#5 Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null.

def activity_count():
    cursor.execute("SELECT transportation_mode, COUNT(*) FROM Activity WHERE transportation_mode IS NOT NULL GROUP BY transportation_mode")
    activities = cursor.fetchall()
    return activities

activities = activity_count()
print(tabulate(activities, headers=["Transportation Mode", "Number of Activities"]))

#6. a) Find the year with the most activities. 
def year_most_activities():
    cursor.execute("""
        SELECT YEAR(start_date_time) as year, COUNT(*) as activities
        FROM Activity
        GROUP BY year
        ORDER BY activities DESC
        LIMIT 1
    """)
    year = cursor.fetchone()
    return year
    
# b)  Is this also the year with most recorded hours?
def year_most_hours():
    cursor.execute("SELECT YEAR(start_date_time) as year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) as hours FROM Activity GROUP BY year ORDER BY hours DESC LIMIT 1")
    year = cursor.fetchone()
    return year

year_activities = year_most_activities()
year_hours = year_most_hours()
print("Year with most activities: ", year_activities)
print("Year with most hours: ", year_hours)

#7. Find the total distance (in km) walked in 2008, by user with id=112.

def total_distance_walked():
    cursor.execute("""
        SELECT TrackPoint.lat, TrackPoint.lon, TrackPoint.date_time
        FROM TrackPoint
        JOIN Activity ON Activity.id = TrackPoint.activity_id
        WHERE Activity.user_id = 112
    """)
    trackpoints = cursor.fetchall()
    distance = 0
    for i in range(1, len(trackpoints)):
        distance += haversine((trackpoints[i-1][0], trackpoints[i-1][1]), (trackpoints[i][0], trackpoints[i][1]), unit=Unit.KILOMETERS)
    return distance

distance_walked = total_distance_walked()
print("Total distance walked in 2008 by user 112: ", distance_walked)

