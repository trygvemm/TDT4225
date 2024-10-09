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
    cursor.execute("""
        SELECT 
            (SELECT COUNT(*) FROM User) AS user_count,
            (SELECT COUNT(*) FROM Activity) AS activity_count,
            (SELECT COUNT(*) FROM TrackPoint) AS trackpoint_count;
    """)
    result = cursor.fetchone()
    users, activities, trackpoints = result
    return users, activities, trackpoints


users_count, activities_count, trackpoints_count = count_users_activities_trackpoints()
print("Number of users: ", users_count)
print("Number of activities: ", activities_count)
print("Number of trackpoints: ", trackpoints_count)

#2. Find the average number of activities per user

def avg_activities_per_user():
    cursor.execute("""
        SELECT 
            (SELECT COUNT(*) FROM Activity) / (SELECT COUNT(*) FROM User) AS avg_activities_per_user;
    """)
    result = cursor.fetchone()[0]
    return result

avg_activities = round(avg_activities_per_user(), 2)
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
users_taxi = [user[0] for user in users_taxi]  # Formatting
print("Users who have taken a taxi:")
print(tabulate([[user] for user in users_taxi], headers=["User ID"]))

#5 Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null.

def activity_count():
    cursor.execute("SELECT transportation_mode, COUNT(*) AS mode_count FROM Activity WHERE transportation_mode IS NOT NULL GROUP BY transportation_mode ORDER BY mode_count DESC")
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

print(f"Year with most activities: {year_activities[0]}, Number of activities: {year_activities[1]}")
print(f"Year with most hours: {year_hours[0]}, Number of hours: {year_hours[1]}")

#7. Find the total distance (in km) walked in 2008, by user with id=112.

# Option 2: Fetche data basert på tidsstempler, ikke id. Gjøre alt av beregninger i SQL.

def total_distance_walked():
    # SQL query to calculate the total distance walked using the Haversine formula
    
    cursor.execute("""
       WITH ordered_points AS (
            SELECT lat AS lat1, lon AS lon1, 
            LEAD(lat) OVER (PARTITION BY activity_id ORDER BY date_time) AS lat2,
            LEAD(lon) OVER (PARTITION BY activity_id ORDER BY date_time) AS lon2
            FROM TrackPoint
            JOIN Activity ON TrackPoint.activity_id = Activity.id
            WHERE Activity.user_id = 112 
            AND YEAR(TrackPoint.date_time) = 2008
            AND Activity.transportation_mode = 'walk'
            )
        SELECT SUM(
            6371 * 2 * ASIN(SQRT(
            POWER(SIN(RADIANS(lat2 - lat1) / 2), 2) +
            COS(RADIANS(lat1)) * COS(RADIANS(lat2)) *
            POWER(SIN(RADIANS(lon2 - lon1) / 2), 2)
            ))
        ) AS total_distance_km
        FROM ordered_points
        WHERE lat2 IS NOT NULL AND lon2 IS NOT NULL;
    """)

    # cursor.execute("""
    #     SELECT SUM(
    #         6371 * 2 * ASIN(SQRT(
    #             POWER(SIN(RADIANS(tp2.lat - tp1.lat) / 2), 2) +
    #             COS(RADIANS(tp1.lat)) * COS(RADIANS(tp2.lat)) *
    #             POWER(SIN(RADIANS(tp2.lon - tp1.lon) / 2), 2)
    #         ))
    #     ) AS total_distance_km
    #     FROM TrackPoint tp1
    #     JOIN TrackPoint tp2 ON tp1.id = tp2.id - 1
    #     JOIN Activity a ON tp1.activity_id = a.id
    #     WHERE a.user_id = 112 
    #       AND YEAR(tp1.date_time) = 2008
    #       AND a.transportation_mode = 'walk'
    #       AND tp1.activity_id = tp2.activity_id;
    # """)
    
    # Fetch the result
    result = cursor.fetchone()

    # Check if result is not None and return the distance
    if result[0] is not None:
        return result[0]
    else:
        return 0

# Call the function and print the result
distance_walked = total_distance_walked()
print("Total distance walked in 2008 by user 112:", round(distance_walked, 2), "kilometers.")

#Find the top 20 users who have gained the most altitude meters.
#Output should be a table with (id, total meters gained per user).
#Remember that some altitude-values are invalid


#TODO: tror dette funker men må gjennom sykt mange trackpoints og tar veldig lang tid å gjøre dette i sql
# def top_20_users_altitude_SQLONLY(): 
#     cursor.execute("""
#        WITH altitude_changes AS (
#             SELECT 
#                 Activity.user_id,
#                 tp.altitude,
#                 LEAD(tp.altitude) OVER (PARTITION BY tp.activity_id ORDER BY tp.date_time) AS next_altitude
#             FROM TrackPoint tp
#             JOIN Activity ON tp.activity_id = Activity.id
#             WHERE tp.altitude IS NOT NULL
#         ),
#         gains AS (
#             SELECT 
#                 user_id,
#                 SUM(CASE 
#                         WHEN next_altitude > altitude THEN next_altitude - altitude 
#                         ELSE 0 
#                     END) AS altitude_gain
#             FROM altitude_changes
#             GROUP BY user_id
#         )
#         SELECT user_id, altitude_gain
#         FROM gains
#         ORDER BY altitude_gain DESC
#         LIMIT 20;
#     """)
#     users = cursor.fetchall()
#     return users

# top_users = top_20_users_altitude_SQLONLY()
# print("Top 20 users by altitude gained:")
# for user_id, total_gain in top_users:
#     print(f"User ID: {user_id}, Total Meters Gained: {total_gain}")
    
def top_20_users_altitude():
    cursor.execute("""
        SELECT 
            a.user_id,
            tp.altitude,
            tp.date_time
        FROM 
            Activity a
        JOIN 
            TrackPoint tp ON a.id = tp.activity_id
        WHERE 
            tp.altitude IS NOT NULL
        ORDER BY 
            a.user_id, tp.date_time;
    """)
    
    results = cursor.fetchall()
    
    # Calculate altitude gains in Python
    altitude_gains = {}
    
    for i in range(len(results)):
        user_id = results[i][0]
        current_altitude = results[i][1]
        
        if user_id not in altitude_gains:
            altitude_gains[user_id] = 0
        
        # Check if there's a next track point for the same user
        if i + 1 < len(results) and results[i + 1][0] == user_id:
            next_altitude = results[i + 1][1]
            if next_altitude > current_altitude:
                # Convert altitude gain from feet to meters
                altitude_gain = (next_altitude - current_altitude) * 0.3048  # Convert to meters
                altitude_gains[user_id] += altitude_gain
    
    # Get top 20 users by altitude gained
    top_users = sorted(altitude_gains.items(), key=lambda x: x[1], reverse=True)[:20]
    
    return top_users

top_users = top_20_users_altitude()
print("Top 20 users by altitude gained (in meters):")
for user_id, total_gain in top_users:
    print(f"User ID: {user_id}, Total Meters Gained: {total_gain:.2f}")


#9. Find all users who have invalid activities. An invalid activity is an activity with consecutive trackpoints where the timestamps deviate with more than 5 minutes.

# #TODO
# def users_invalid_activities():
#     cursor.execute("""
#         SELECT DISTINCT User.id
#         FROM User
#         JOIN Activity ON User.id = Activity.user_id
#         JOIN TrackPoint tp1 ON Activity.id = tp1.activity_id
#         JOIN TrackPoint tp2 ON tp1.activity_id = tp2.activity_id AND tp1.date_time < tp2.date_time
#         WHERE TIMESTAMPDIFF(MINUTE, tp1.date_time, tp2.date_time) > 5
#     """)
#     users = cursor.fetchall()
#     return users

# users_invalid = users_invalid_activities()
# print("Users with invalid activities: ", users_invalid)



#10. Find the users who have tracked an activity in the Forbidden City of Beijing.
# In this question you can consider the Forbidden City to have coordinates that correspond to: lat 39.916, lon 116.397.

def users_forbidden_city():
    cursor.execute("""
        SELECT DISTINCT User.id
        FROM User
        JOIN Activity ON User.id = Activity.user_id
        JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
        WHERE TrackPoint.lat LIKE '39.916%' 
          AND TrackPoint.lon LIKE '116.397%'
    """)
    users = cursor.fetchall()
    return users

users_forbidden = users_forbidden_city()
users_forbidden = [user[0] for user in users_forbidden]  # Formatting
print("Users who have tracked an activity in the Forbidden City of Beijing:")
print(tabulate([[user] for user in users_forbidden], headers=["User ID"]))

#11.Find all users who have registered transportation_mode and their most used transportation_mode.
# ○ The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id.
# ○ Some users may have the same number of activities tagged with e.g. walk and car. In this case it is up to you to decide which transportation mode to include in your answer (choose one).
# ○ Do not count the rows where the mode is null

def users_most_used_transportation_mode():
    cursor.execute("""
        SELECT user_id, transportation_mode
        FROM (
            SELECT user_id, transportation_mode, COUNT(*) AS mode_count
            FROM Activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id, transportation_mode
        ) AS mode_counts
        WHERE mode_count = (
            SELECT MAX(mode_count)
            FROM (
                SELECT user_id, transportation_mode, COUNT(*) AS mode_count
                FROM Activity
                WHERE transportation_mode IS NOT NULL
                GROUP BY user_id, transportation_mode
            ) AS inner_counts
            WHERE inner_counts.user_id = mode_counts.user_id
        )
        ORDER BY user_id
    """)
    users = cursor.fetchall()
    return users




users_most_used = users_most_used_transportation_mode()
print("Users and their most used transportation mode:")
print(tabulate(users_most_used, headers=["User ID", "Most Used Transportation Mode"]))
