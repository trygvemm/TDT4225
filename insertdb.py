from DbConnector import DbConnector
import os

class InsertDB:
    # Connects to the database
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    def truncate_tables(self):
        try:
            # Truncate tables in reverse order due to foreign key constraints
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")  # Temporarily disable foreign key checks
            self.cursor.execute("TRUNCATE TABLE TrackPoint;")
            self.cursor.execute("TRUNCATE TABLE Activity;")
            self.cursor.execute("TRUNCATE TABLE User;")
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")  # Re-enable foreign key checks
            self.db_connection.commit()
            print("All tables truncated successfully.")
        except Exception as e:
            print(f"Error truncating tables: {e}")
    
    # Creates the tables in the database
    def create_tables(self):
        query = """CREATE TABLE IF NOT EXISTS User (
                id VARCHAR(255) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN)
                """
        self.cursor.execute(query)
        self.db_connection.commit()

        query = """CREATE TABLE IF NOT EXISTS Activity (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                user_id VARCHAR(255),
                transportation_mode VARCHAR(255),
                start_date_time DATETIME,
                end_date_time DATETIME,
                FOREIGN KEY (user_id) REFERENCES User(id))
                """
        self.cursor.execute(query)
        self.db_connection.commit()

        query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id INT,
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,  -- altitude in meters
                date_days DOUBLE,
                date_time DATETIME,
                FOREIGN KEY (activity_id) REFERENCES Activity(id))
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    # Inserts users into the database
    def insert_user(self, user_id, has_labels):
        # Check if the user already exists
        query = "SELECT COUNT(*) FROM User WHERE id = %s"
        self.cursor.execute(query, (user_id,))
        result = self.cursor.fetchone()

        if result[0] == 0:
            # If the user does not exist, insert them
            query = "INSERT INTO User (id, has_labels) VALUES (%s, %s)"
            self.cursor.execute(query, (user_id, has_labels))
            self.db_connection.commit()
        else:
            # If the user already exists, skip insertion
            print(f"User {user_id} already exists, skipping insertion.")


    # Inserts activities into the database
    def insert_activity(self, user_id, transportation_mode, start_date_time, end_date_time):
        query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"
        self.cursor.execute(query, (user_id, transportation_mode, start_date_time, end_date_time))
        self.db_connection.commit()
        return self.cursor.lastrowid  # Return the ID of the inserted activity

    # Inserts trackpoints into the database
    def insert_trackpoint(self, activity_id, lat, lon, altitude, date_days, date_time):
        query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
        self.cursor.execute(query, (activity_id, lat, lon, altitude, date_days, date_time))
        self.db_connection.commit()

    def insert_trackpoints_batch(self, trackpoints):
        query = """
            INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.cursor.executemany(query, trackpoints)
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s" % table_name
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

import time

def read_plt_files_and_insert(data_directory, db_handler):
    batch_size = 1000  # Set a batch size for batch insertion
    trackpoint_batch = []  # List to hold trackpoints for batch insert
    
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
                    
                    # Insert user if not already inserted
                    db_handler.insert_user(user_id, has_labels=False)

                    # Set start and end times for the activity
                    start_time = lines[0].strip().split(',')[-2] + ' ' + lines[0].strip().split(',')[-1]
                    end_time = lines[-1].strip().split(',')[-2] + ' ' + lines[-1].strip().split(',')[-1]

                    # Insert activity and get the activity ID
                    activity_id = db_handler.insert_activity(user_id, None, start_time, end_time)

                    # Insert each trackpoint for the current activity
                    for line in lines:
                        lat, lon, _, altitude_feet, date_days, date, time = line.strip().split(',')

                        # Convert altitude from feet to meters
                        altitude_meters = int(float(altitude_feet) * 0.3048)

                        # Add trackpoint data to the batch
                        trackpoint_batch.append((activity_id, float(lat), float(lon), altitude_meters, float(date_days), f"{date} {time}"))

                        # Once the batch size is reached, insert the batch
                        if len(trackpoint_batch) >= batch_size:
                            db_handler.insert_trackpoints_batch(trackpoint_batch)
                            trackpoint_batch.clear()  # Clear the batch after insertion

    # Insert any remaining trackpoints in the last batch
    if trackpoint_batch:
        db_handler.insert_trackpoints_batch(trackpoint_batch)

# Usage
db_handler = InsertDB()
db_handler.create_tables()
read_plt_files_and_insert("dataset2/Data", db_handler)