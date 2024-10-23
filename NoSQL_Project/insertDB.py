from DbConnector import DbConnector
import os
from datetime import datetime

class InsertMongoDB:
    # Connects to the MongoDB database
    def __init__(self):
        self.connection = DbConnector()
        self.db = self.connection.db

    # Drops collections if they exist to reset data
    def drop_collections(self):
        self.db.TrackPoint.drop()
        self.db.Activity.drop()
        self.db.User.drop()
        print("All collections dropped successfully.")

    # Inserts users into the database
    def insert_user(self, user_id, has_labels):
        user = {
            "_id": user_id,
            "has_labels": has_labels
        }
        self.db.User.insert_one(user)
        # print(f"Inserted user: {user_id}")

    # Inserts an activity and returns its ID
    def insert_activity(self, user_id, transportation_mode, start_date_time, end_date_time):
        activity = {
            "user_id": user_id,
            "transportation_mode": transportation_mode,
            "start_date_time": start_date_time,
            "end_date_time": end_date_time
        }
        result = self.db.Activity.insert_one(activity)
        return result.inserted_id  # Return the ID of the inserted activity

    # Inserts trackpoints in bulk
    def insert_trackpoints_batch(self, trackpoints):
        if trackpoints:
            self.db.TrackPoint.insert_many(trackpoints)
            # print(f"Inserted batch of {len(trackpoints)} trackpoints.")


def read_plt_files_and_insert(data_directory, db_handler, labeled_ids):
    batch_size = 5000  # Batch size for batch insertion of trackpoints
    trackpoint_batch = []  # List to hold trackpoints for batch insert
    users_to_insert = set()  # Set to hold unique users to insert

    def format_time(time_str, from_format, to_format):
        return datetime.strptime(time_str, from_format).strftime(to_format)

    # First pass: Collect all user IDs from labeled_ids and activity files
    for root, dirs, files in os.walk(data_directory):
        for file in files:
            if file.endswith(".plt"):
                user_id = os.path.basename(os.path.dirname(root))  # Get the user directory (001, 002, etc.)
                # Collect user for the activity
                users_to_insert.add((user_id, user_id in labeled_ids))  # Add user and whether they have labels

    # Insert unique users into the database before processing activities
    for user_id, has_labels in users_to_insert:
        db_handler.insert_user(user_id, has_labels)

    # Second pass: Process files and insert activities and trackpoints
    for root, dirs, files in os.walk(data_directory):
        for file in files:
            if file.endswith(".plt"):
                user_id = os.path.basename(os.path.dirname(root))  # Get the user directory (001, 002, etc.)
                file_path = os.path.join(root, file)

                with open(file_path, "r") as f:
                    lines = f.readlines()[6:]  # Skip the first 6 header lines

                    # Skip the file if it has more than 2500 trackpoints
                    if len(lines) > 2500:
                        continue

                    # Set start and end times for the activity
                    start_time_raw = lines[0].strip().split(',')[-2] + ' ' + lines[0].strip().split(',')[-1]
                    end_time_raw = lines[-1].strip().split(',')[-2] + ' ' + lines[-1].strip().split(',')[-1]

                    # Convert start and end times to datetime objects
                    start_time = datetime.strptime(start_time_raw, '%Y-%m-%d %H:%M:%S')
                    end_time = datetime.strptime(end_time_raw, '%Y-%m-%d %H:%M:%S')

                    transportation_mode = None

                    # Load label data if user_id matches
                    if user_id in labeled_ids:
                        label_data = []
                        label_file_path = f"SQL_Project\\dataset\\Data\\{user_id}\\labels.txt"
                        if os.path.exists(label_file_path):
                            with open(label_file_path, "r") as label_file:
                                for line in label_file.readlines()[1:]:  # Skip header
                                    label_start, label_end, mode = line.strip().split('\t')
                                    label_data.append((label_start, label_end, mode))
                            # Check for matching start and end times in label_data
                            for label_start, label_end, mode in label_data:
                                # Convert label times to match the datetime format for comparison
                                label_start_dt = datetime.strptime(label_start, '%Y/%m/%d %H:%M:%S')
                                label_end_dt = datetime.strptime(label_end, '%Y/%m/%d %H:%M:%S')
                                if label_start_dt == start_time and label_end_dt == end_time:
                                    # print("Match found for transportation mode!")
                                    transportation_mode = mode
                                    break

                    # Insert activity and get the activity ID
                    activity_id = db_handler.insert_activity(user_id, transportation_mode, start_time, end_time)

                    # Insert each trackpoint for the current activity
                    for line in lines:
                        lat, lon, _, altitude_feet, _, date, time = line.strip().split(',')

                        # Convert altitude from feet to meters
                        altitude_int = int(float(altitude_feet))

                        # Add trackpoint data to the batch if altitude is valid
                        if altitude_int != -777:
                            trackpoint_batch.append({
                                "activity_id": activity_id,
                                "lat": float(lat),
                                "lon": float(lon),
                                "altitude": altitude_int,
                                "date_time": datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M:%S')
                            })

                        # Once the batch size is reached, insert the batch
                        if len(trackpoint_batch) >= batch_size:
                            db_handler.insert_trackpoints_batch(trackpoint_batch)
                            trackpoint_batch.clear()  # Clear the batch after insertion

    # Insert any remaining trackpoints in the last batch
    if trackpoint_batch:
        db_handler.insert_trackpoints_batch(trackpoint_batch)


# Reads labeled_ids.txt
def read_numbers_from_file(file_path):
    numbers = []
    with open(file_path, "r") as file:
        for line in file:
            for word in line.split():
                numbers.append(word)
    return numbers

# Usage
file_path = "SQL_Project\\dataset\\labeled_ids.txt"
numbers_list = read_numbers_from_file(file_path)
db_handler = InsertMongoDB()
db_handler.drop_collections()
read_plt_files_and_insert("SQL_Project\\dataset\\Data", db_handler, numbers_list)

db_handler.connection.close_connection()
