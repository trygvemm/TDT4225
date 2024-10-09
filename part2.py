# Part 2 of the assignment
from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine, Unit

connection = DbConnector()
db_connection = connection.db_connection
cursor = connection.cursor

# 1. How many users, activities, and trackpoints are there in the dataset (after it is inserted into the database)
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
print("\nTask 1: Number of Users, Activities, and Trackpoints")
print(tabulate([["Users", users_count], ["Activities", activities_count], ["Trackpoints", trackpoints_count]], headers=["Entity", "Count"], tablefmt="grid"))

# 2. Find the average number of activities per user
def avg_activities_per_user():
    cursor.execute("""
        SELECT 
            (SELECT COUNT(*) FROM Activity) / (SELECT COUNT(*) FROM User) AS avg_activities_per_user;
    """)
    result = cursor.fetchone()[0]
    return result

avg_activities = round(avg_activities_per_user(), 2)
print("\nTask 2: Average Number of Activities per User")
print(f"Average number of activities per user: {avg_activities}")

# 3. Find the top 20 users with the highest number of activities
def top_20_users_highest_activities():
    cursor.execute("""
        SELECT User.id, COUNT(Activity.id) as activities 
        FROM User 
        JOIN Activity ON User.id = Activity.user_id 
        GROUP BY User.id 
        ORDER BY activities DESC 
        LIMIT 20
    """)
    users = cursor.fetchall()
    return users

top_20_users = top_20_users_highest_activities()
print("\nTask 3: Top 20 Users with the Highest Number of Activities")
print(tabulate(top_20_users, headers=["User ID", "Number of Activities"], tablefmt="grid"))

# 4. Find all users who have taken a taxi
def users_taken_taxi():
    cursor.execute("""
        SELECT DISTINCT User.id 
        FROM User 
        JOIN Activity ON User.id = Activity.user_id 
        WHERE Activity.transportation_mode = 'taxi'
    """)
    users = cursor.fetchall()
    return users

users_taxi = users_taken_taxi()
users_taxi = [user[0] for user in users_taxi]  # Formatting
print("\nTask 4: Users Who Have Taken a Taxi")
print(tabulate([[user] for user in users_taxi], headers=["User ID"], tablefmt="grid"))

# 5. Find all types of transportation modes and count how many activities are tagged with these transportation mode labels
def activity_count():
    cursor.execute("""
        SELECT transportation_mode, COUNT(*) AS mode_count 
        FROM Activity 
        WHERE transportation_mode IS NOT NULL 
        GROUP BY transportation_mode 
        ORDER BY mode_count DESC
    """)
    activities = cursor.fetchall()
    return activities

activities = activity_count()
print("\nTask 5: Transportation Modes and Their Activity Counts")
print(tabulate(activities, headers=["Transportation Mode", "Number of Activities"], tablefmt="grid"))

# 6. a) Find the year with the most activities
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

# b) Is this also the year with the most recorded hours?
def year_most_hours():
    cursor.execute("""
        SELECT YEAR(start_date_time) as year, SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) as hours 
        FROM Activity 
        GROUP BY year 
        ORDER BY hours DESC 
        LIMIT 1
    """)
    year = cursor.fetchone()
    return year

year_activities = year_most_activities()
year_hours = year_most_hours()
print("\nTask 6: Year with Most Activities vs. Year with Most Recorded Hours")
print(tabulate([
    ["Most Activities", year_activities[0], year_activities[1], "N/A"],
    ["Most Hours", year_hours[0], "N/A", year_hours[1]]
], headers=["Metric", "Year", "Number of Activities", "Number of Hours"], tablefmt="grid"))

# 7. Find the total distance (in km) walked in 2008 by user with id=112
def total_distance_walked():
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
    result = cursor.fetchone()
    if result[0] is not None:
        return result[0]
    else:
        return 0

distance_walked = total_distance_walked()
print("\nTask 7: Total Distance Walked in 2008 by User 112")
print(f"Total distance walked by user 112 in 2008: {round(distance_walked, 2)} kilometers")

# 8. Find the top 20 users who have gained the most altitude meters
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
    last_altitude = {}
    
    for row in results:
        user_id = row[0]
        current_altitude = row[1]
        
        if user_id not in last_altitude:
            last_altitude[user_id] = current_altitude
            altitude_gains[user_id] = 0
            continue
        
        altitude_diff = current_altitude - last_altitude[user_id]
        if altitude_diff > 0:
            altitude_gains[user_id] += altitude_diff
        last_altitude[user_id] = current_altitude
    
    # Convert altitude gain from feet to meters
    for user_id in altitude_gains:
        altitude_gains[user_id] *= 0.3048  # Convert to meters
    
    # Get top 20 users by altitude gained
    top_users = sorted(altitude_gains.items(), key=lambda x: x[1], reverse=True)[:20]
    return top_users

top_users = top_20_users_altitude()
print("\nTask 8: Top 20 Users by Altitude Gained (in meters)")
print(tabulate([[user_id, f"{total_gain:.2f}"] for user_id, total_gain in top_users], headers=["User ID", "Total Meters Gained"], tablefmt="grid"))

# 9. Find all users who have invalid activities
def find_invalid_activities_sql():
    query = """
    WITH trackpoint_diffs AS (
        SELECT
            tp.activity_id,
            a.user_id,
            tp.date_time AS curr_time,
            LAG(tp.date_time) OVER (PARTITION BY tp.activity_id ORDER BY tp.date_time) AS prev_time
        FROM
            TrackPoint tp
            JOIN Activity a ON tp.activity_id = a.id
    ),
    invalid_activities AS (
        SELECT
            activity_id,
            user_id
        FROM
            trackpoint_diffs
        WHERE
            prev_time IS NOT NULL
            AND TIMESTAMPDIFF(MINUTE, prev_time, curr_time) >= 5
        GROUP BY
            activity_id,
            user_id
    )
    SELECT
        user_id,
        COUNT(DISTINCT activity_id) AS invalid_activity_count
    FROM
        invalid_activities
    GROUP BY
        user_id
    ORDER BY
        invalid_activity_count DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()

    # Ensure user IDs are strings with leading zeros if necessary
    formatted_results = [(str(user_id).zfill(3), count) for user_id, count in results]

    print("\nTask 9: Users with Invalid Activities")
    print(tabulate(formatted_results, headers=["User ID", "Invalid Activities"], tablefmt="grid"))

# Call the function
find_invalid_activities_sql()

# 10. Find the users who have tracked an activity in the Forbidden City of Beijing
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
print("\nTask 10: Users Who Visited the Forbidden City of Beijing")
print(tabulate([[user] for user in users_forbidden], headers=["User ID"], tablefmt="grid"))

# 11. Find all users who have registered transportation_mode and their most used transportation_mode
def users_most_used_transportation_mode():
    cursor.execute("""
        SELECT user_id, transportation_mode
        FROM (
            SELECT user_id, transportation_mode, COUNT(*) AS mode_count,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rn
            FROM Activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id, transportation_mode
        ) AS mode_counts
        WHERE rn = 1  -- Only select the most used transportation mode for each user
        ORDER BY user_id
    """)
    users = cursor.fetchall()
    return users

users_most_used = users_most_used_transportation_mode()
print("\nTask 11: Users and Their Most Used Transportation Mode")
print(tabulate(users_most_used, headers=["User ID", "Most Used Transportation Mode"], tablefmt="grid"))