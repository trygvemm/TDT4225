from pymongo import MongoClient
from tabulate import tabulate
from DbConnector import DbConnector
from datetime import datetime

# Establishing the connection to MongoDB using DbConnector
connection = DbConnector()  # Create an instance of DbConnector
db_connection = connection.db  # Access the database connection from the DbConnector instance
activity_collection = db_connection['Activity']  # Replace with your collection name
user_collection = db_connection['User']  # Replace with your collection name
trackpoint_collection = db_connection['TrackPoint']  # Replace with your collection name


# 1. Count the number of users, activities, and trackpoints in the dataset
def count_users_activities_trackpoints():
    user_count = user_collection.count_documents({})
    activity_count = activity_collection.count_documents({})
    trackpoint_count = trackpoint_collection.count_documents({})
    return user_count, activity_count, trackpoint_count

users_count, activities_count, trackpoints_count = count_users_activities_trackpoints()
print("\nTask 1: Number of Users, Activities, and Trackpoints")
print(tabulate([["Users", users_count], ["Activities", activities_count], ["Trackpoints", trackpoints_count]], headers=["Entity", "Count"], tablefmt="grid"))

# 2. Find the average number of activities per user
def avg_activities_per_user():
    activity_count = activity_collection.count_documents({})
    user_count = user_collection.count_documents({})
    return round(activity_count / user_count, 2) if user_count > 0 else 0

avg_activities = avg_activities_per_user()
print("\nTask 2: Average Number of Activities per User")
print(f"Average number of activities per user: {avg_activities}")

# 3. Find the top 20 users with the highest number of activities
def top_20_users_highest_activities():
    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "activities": {"$sum": 1}
            }
        },
        {
            "$sort": {"activities": -1}
        },
        {
            "$limit": 20
        }
    ]
    return list(activity_collection.aggregate(pipeline))

top_20_users = top_20_users_highest_activities()
print("\nTask 3: Top 20 Users with the Highest Number of Activities")
print(tabulate([[user["_id"], user["activities"]] for user in top_20_users], headers=["User ID", "Number of Activities"], tablefmt="grid"))

# 4. Find all users who have taken a taxi
def users_taken_taxi():
    users = activity_collection.distinct("user_id", {"transportation_mode": "taxi"})
    return users

users_taxi = users_taken_taxi()
print("\nTask 4: Users Who Have Taken a Taxi")
print(tabulate([[user] for user in users_taxi], headers=["User ID"], tablefmt="grid"))

# 5. Find all types of transportation modes and count how many activities are tagged with these transportation mode labels
def activity_count():
    pipeline = [
        {
            "$match": {
                "transportation_mode": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": "$transportation_mode",
                "mode_count": {"$sum": 1}
            }
        },
        {
            "$sort": {"mode_count": -1}
        }
    ]
    return list(activity_collection.aggregate(pipeline))

activities = activity_count()
print("\nTask 5: Transportation Modes and Their Activity Counts")
print(tabulate([[activity["_id"], activity["mode_count"]] for activity in activities], headers=["Transportation Mode", "Number of Activities"], tablefmt="grid"))

# 6. Find the year with the most activities recorded
def year_most_activities():
    pipeline = [
        {
            "$group": {
                "_id": {"$year": "$start_date_time"},
                "activities": {"$sum": 1}
            }
        },
        {
            "$sort": {"activities": -1}
        },
        {
            "$limit": 1
        }
    ]
    return list(activity_collection.aggregate(pipeline))

# b) Find the year with the most hours recorded
def year_most_hours():
    pipeline = [
        {
            "$group": {
                "_id": {"$year": "$start_date_time"},
                "hours": {
                    "$sum": {
                        "$divide": [
                            {"$subtract": ["$end_date_time", "$start_date_time"]},
                            3600000
                        ]  # Convert ms to hours
                    }
                }
            }
        },
        {
            "$sort": {"hours": -1}
        },
        {
            "$limit": 1
        }
    ]
    return list(activity_collection.aggregate(pipeline))

year_activities = year_most_activities()
year_hours = year_most_hours()

print("\nTask 6: Years with most activities and hours recorded")
print(f"a) {list(year_activities)[0]['_id']} had the most recorded activities with {list(year_activities)[0]['activities']} activities.")
print(f"b) {list(year_hours)[0]['_id']} had the most recorded hours with {list(year_hours)[0]['hours']} hours.")

# 7. Find the total distance (in km) walked in 2008 by user with id=112
def total_distance_walked():
    # Define the date range for the year 2008
    start_date = datetime(2008, 1, 1)
    end_date = datetime(2009, 1, 1)

    # Sum up total distances for walking activities in 2008 for user 112 and convert feet to kilometers
    pipeline = [
        {
            "$match": {
                "user_id": "112",
                "transportation_mode": "walk",
                "start_date_time": {"$gte": start_date, "$lt": end_date}
            }
        },
        {
            "$group": {
                "_id": None,
                # Convert from feet to kilometers by multiplying with 0.0003048
                "total_distance_km": {"$sum": {"$multiply": ["$total_distance", 0.0003048]}}
            }
        }
    ]
    result = list(activity_collection.aggregate(pipeline))
    total_distance_all_activities = result[0]["total_distance_km"] if result else 0

    return total_distance_all_activities

# Call the function to get the distance walked
distance_walked = total_distance_walked()
print("\nTask 7: Total Distance Walked in 2008 by User 112")
print(f"Total distance walked by user 112 in 2008: {round(distance_walked, 2)} kilometers")




# 8. Find the top 20 users who have gained the most altitude meters
def top_20_users_altitude():
    pipeline = [
        # Match only documents where altitude_gained exists and is greater than 0
        {
            "$match": {
                "altitude_gain": {"$exists": True, "$gt": 0}
            }
        },
        # Convert altitude from feet to meters
        {
            "$project": {
                "user_id": 1,
                "altitude_gain_meters": {"$multiply": ["$altitude_gain", 0.3048]}
            }
        },
        # Group by user_id and calculate total altitude gain in meters
        {
            "$group": {
                "_id": "$user_id",
                "total_gain": {"$sum": "$altitude_gain_meters"}
            }
        },
        # Sort by total_gain in descending order
        {
            "$sort": {"total_gain": -1}
        },
        # Limit to the top 20 users
        {
            "$limit": 20
        }
    ]
    return list(activity_collection.aggregate(pipeline))

top_users = top_20_users_altitude()
print("\nTask 8: Top 20 Users by Altitude Gained (in meters)")
print(tabulate([[user["_id"], f"{user['total_gain']:.2f}"] for user in top_users], headers=["User ID", "Total Meters Gained"], tablefmt="grid"))

# 9. Find all users who have invalid activities
def find_invalid_activities():
    pipeline = [
        {
            "$match": {
                "is_valid": False
            }
        },
        {
            "$group": {
                "_id": "$user_id",
                "invalid_activity_count": {"$sum": 1}
            }
        },
        {
            "$sort": {"invalid_activity_count": -1}
        }
    ]
    return list(activity_collection.aggregate(pipeline))

invalid_users = find_invalid_activities()
print("\nTask 9: Users with Invalid Activities")
print(tabulate([[user["_id"], user["invalid_activity_count"]] for user in invalid_users], headers=["User ID", "Invalid Activities"], tablefmt="grid"))

# 10. Find the users who have tracked an activity in the Forbidden City of Beijing
def users_forbidden_city():
    users = trackpoint_collection.distinct("user_id", {
        "lat": {"$gte": 39.916, "$lt": 39.917},  # Adjust to your required precision
        "lon": {"$gte": 116.397, "$lt": 116.398}
    })
    return users

users_forbidden = users_forbidden_city()
print("\nTask 10: Users Who Visited the Forbidden City of Beijing")
print(tabulate([[user] for user in users_forbidden], headers=["User ID"], tablefmt="grid"))

# 11. Find all users who have registered transportation_mode and their most used transportation_mode
def users_most_used_transportation_mode():
    pipeline = [
        {
            "$group": {
                "_id": {"user_id": "$user_id", "transportation_mode": "$transportation_mode"},
                "mode_count": {"$sum": 1}
            }
        },
        {
            "$sort": {"mode_count": -1}
        },
        {
            "$group": {
                "_id": "$_id.user_id",
                "most_used_mode": {"$first": "$_id.transportation_mode"}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]
    return list(activity_collection.aggregate(pipeline))

users_most_used = users_most_used_transportation_mode()
print("\nTask 11: Users and Their Most Used Transportation Mode")
print(tabulate([[user["_id"], user["most_used_mode"]] for user in users_most_used], headers=["User ID", "Most Used Transportation Mode"], tablefmt="grid"))
