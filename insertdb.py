from DbConnector import DbConnector
import tabulate
import os

class InsertDB:

    #Connects to the database
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
    
    #Creates the tables in the database
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
                altitude INT,
                date_days DOUBLE,
                date_time DATETIME,
                FOREIGN KEY (activity_id) REFERENCES Activity(id))
                """
        self.cursor.execute(query)
        self.db_connection.commit()
    
    def insert_users(self, users):
        for user in users:
            query = "INSERT INTO User (id, has_labels) VALUES ('%s', %s)"
            self.cursor.execute(query % (user, users[user]))
        self.db_connection.commit()
    
    def insert_activities(self, activities):
        for activity in activities:
            query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES ('%s', '%s', '%s', '%s')"
            self.cursor.execute(query % (activity[0], activity[1], activity[2], activity[3]))
        self.db_connection.commit()

    def insert_trackpoints(self, trackpoints):
        for trackpoint in trackpoints:
            query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, '%s')"
            self.cursor.execute(query % (trackpoint[0], trackpoint[1], trackpoint[2], trackpoint[3], trackpoint[4], trackpoint[5]))
        self.db_connection.commit()
    
    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        return rows
    
    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
    

import os

def read_plt_files_to_dict(data_directory):
    data_dict = {}
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
                    
                    trackpoints = []
                    for line in lines:
                        lat, lon, _, altitude, date_days, date, time = line.strip().split(',')
                        trackpoints.append({
                            "lat": float(lat),
                            "lon": float(lon),
                            "altitude": float(altitude),
                            "date_days": float(date_days),
                            "date_time": f"{date} {time}"
                        })
                    
                    # Store the trackpoints in the dictionary, key is user_id + activity_id
                    data_dict[f"{user_id}_{file}"] = trackpoints
    return data_dict


#test data_dict
data_dict = read_plt_files_to_dict("dataset/Data")
print(data_dict)