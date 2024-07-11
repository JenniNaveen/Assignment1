from googleapiclient.discovery import build
import pandas as pd
import streamlit as st
import mysql.connector as db
from mysql.connector import Error
from datetime import datetime

c_id='UCJHw4jqOkib-TmCnyGra94Q'       # ChannelID

def Api_connect():
  Api_ID = 'AIzaSyB__KpjJdmwrKntWKnWTz2x_0sNnNjb_gs'
  api_service_name = "youtube"
  api_version = "v3"
  youtube = build(api_service_name,api_version,developerKey=Api_ID)
  return youtube
youtube=Api_connect()


def get_channel_info(c) :
  request = youtube.channels().list (
    part="snippet,contentDetails,statistics",
       id=c
  )
  response = request.execute()
  for i in response ['items']:
     data = dict(channel_name = i['snippet']['title'],
             channel_id=i['id'],
             subscribers = i['statistics']['subscriberCount'],
             views=i['statistics']['viewCount'],
             channel_description =i['snippet']['description'],
             total_videos = i['statistics']['videoCount'],
             playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
     )

 
  return data

def get_video_ids(channel_id):
     Video_ids=[]
     response=youtube.channels().list(id=channel_id,
                                part='contentDetails').execute()
     playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
     next_page_token=None
 
     while True:
          response1=youtube.playlistItems().list(part='snippet',
                                            playlistId=playlist_Id,
                                             maxResults=50,                             
                                            pageToken=next_page_token).execute()
          for i in range(len(response1['items'])):
           Video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
          next_page_token = response1.get('nextPageToken')
          
          if next_page_token is None:
                  break
     return Video_ids

# get video information
def get_video_info(video_Ids):
 video_data=[]
 for video_id in video_Ids:
     request=youtube.videos().list(
          part="snippet,ContentDetails,statistics",
          id=video_id
     )
     response=request.execute()

     for item in response["items"]:
          data=dict(channel_name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=",".join(item['snippet'].get('tags',["na"])),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['statistics'].get('duration'),
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favourite_Count=item['statistics'].get('favouriteCount'),
                    Definition=item['contentDetails'].get('definition'),
                    Caption_Status=item['contentDetails'].get('caption')
                    )
          video_data.append(data)

 return  video_data      

def get_comment_info(video_Ids):
    comment_data=[]
    try:
        for video_id in video_Ids:
          request=youtube.commentThreads().list(
                  part="snippet",
                  videoId=video_id,
                  maxResults=50
                )
          response=request.execute()
          for item in response['items']:
                      data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                           Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                           Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                           Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                           Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                      comment_data.append(data)
                      
                      
    except:
      pass
    return comment_data

#get playlist details
def get_palylist_details(channel_id):
     next_page_token=None
     All_data=[]
     while True:
            request=youtube.playlists().list(
                  part='snippet,contentDetails',
                  channelId=channel_id,
                  maxResults=50,
                  pageToken=next_page_token
               )
            response=request.execute()

            for item in response['items']:
              data=dict(Playlist_Id=item['id'],
               Title=item['snippet']['title'],
               Channel_Id=item['snippet']['channelId'],
               Channel_Name=item['snippet']['channelTitle'],
               PublishedAt=item['snippet']['publishedAt'],
               Video_count=item['contentDetails']['itemCount'])
              All_data.append(data)
            next_page_token=response.get('nextPageToken')

            if next_page_token is None:
             break
     return All_data 


channel_dict=get_channel_info(c_id)
channel_details= [channel_dict]
video_Ids=get_video_ids(c_id)
video_details=get_video_info(video_Ids)
commment_details=get_comment_info(video_Ids)
playlist_details=get_palylist_details(c_id)

def convert_iso_to_mysql_datetime(iso_datetime):
    # Convert ISO formatted datetime string to Python datetime object
    dt_object = datetime.strptime(iso_datetime, '%Y-%m-%dT%H:%M:%SZ')
    # Convert Python datetime object to MySQL DATETIME format string
    mysql_datetime = dt_object.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_datetime


def create_Insert_channeldetails_table():
    try:
        # Connect to MySQL server
        connection = db.connect(
            host='localhost',
            database='youtube',
            user='root',
            password='root'
        )

        if connection.is_connected():
            # Create a cursor object
            cursor = connection.cursor()

            # Define your table creation SQL query
            create_table_query = """
            CREATE TABLE IF NOT EXISTS Channel_details (
            channel_id         varchar(255) primary key,
            channel_name        varchar(255),
            subscribers         varchar(200),
            channel_views       varchar(200),
            channel_description varchar(2000),
            Total_videos        varchar(200),
            Playlist_ID         Varchar(100)
           )
            """

            # Execute the table creation query
            cursor.execute(create_table_query)
            print("Table created successfully!")

    except Error as e:
        # Handle errors
        print("channel table already exists")
   
    try:   
        
        
        
            # Define the INSERT query
            insert_query = """
            INSERT INTO channel_details (channel_name, channel_id,subscribers, channel_views,channel_description,total_videos,playlist_id)
            VALUES (%s, %s, %s,%s,%s,%s,%s)
            """
        
            # Execute the INSERT query for each channel detail
            for channel in channel_details:
                cursor.execute(insert_query, (channel['channel_name'], channel['channel_id'],channel['subscribers'], channel['views'],channel['channel_description'], channel['total_videos'],channel['playlist_id'] ))
                print(f"Inserted {channel['channel_name']} successfully!")

            # Commit the transaction
            connection.commit()
            
    
    except:
        # Handle errors
        print("Channels details already Inserted")        

    finally:
        # Close the database connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Call the function to create the table
create_Insert_channeldetails_table() 



def create_Insert_playlistdetails_table():
    try:
        # Connect to MySQL server
        connection = db.connect(
            host='localhost',
            database='youtube',
            user='root',
            password='root'
        )

        if connection.is_connected():
            # Create a cursor object
            cursor = connection.cursor()

            # Define your table creation SQL query
            create_table_query = """
            CREATE TABLE IF NOT EXISTS Playlists_details (
            Playlist_Id varchar(255) primary key,
            Title   varchar(2000),
            Channel_Id varchar(255),
            Channel_Name varchar(255),
            PublishedAt TIMESTAMP,
            Video_count Integer
           )
            """

            # Execute the table creation query
            cursor.execute(create_table_query)
            print("Table created successfully!")

            # Define the INSERT query
            insert_query = """
            INSERT INTO Playlists_details (Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_count)
            VALUES (%s, %s, %s, %s, %s, %s)
            """


            # Execute the INSERT query for each playlist detail
            for playlist in playlist_details:
                try:
                    # Convert ISO 8601 datetime string to MySQL-compatible datetime object
                    published_at = convert_iso_to_mysql_datetime(playlist['PublishedAt'])
                    cursor.execute(insert_query, (
                        playlist['Playlist_Id'],
                        playlist['Title'],
                        playlist['Channel_Id'],
                        playlist['Channel_Name'],
                        published_at,
                        playlist['Video_count']
                    ))
                    print("Inserted Playlists Details successfully!")
                except Error as e:
                    print(f"Error inserting playlist {playlist['Playlist_Id']}: {e}")

            # Commit the transaction
            connection.commit()

    except Error as e:
        # Handle errors
        print("Error while connecting to MySQL", e)

    finally:
        # Close the database connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Call the function to create the table and insert data
create_Insert_playlistdetails_table()



def create_and_insert_video_details():
    try:
        # Connect to MySQL server
        connection = db.connect(
            host='localhost',
            database='youtube',
            user='root',
            password='root'
        )

        if connection.is_connected():
            # Create a cursor object
            cursor = connection.cursor()

            # Define your table creation SQL query
            create_table_query = """
            CREATE TABLE IF NOT EXISTS Video_details (
            Video_Id            varchar(255) primary key,
            channel_id          varchar(255) references channel_details(channel_id),
            channel_name        varchar(255),
            Title               varchar(200),
            Tags                varchar(200),
            Thumbnail           BLOB,
            Description         varchar(100000),
            Published_Date      TIMESTAMP,
            Duration            varchar(50),
            Views               Integer,
            Likes               Integer,
            Comments            Integer,
            Favourite_Count     Integer,
            Definition          varchar(200),
            Caption_Status      varchar(200)
           )
            """

            # Execute the table creation query
            cursor.execute(create_table_query)
            print("Table created successfully!")

            # Define the INSERT query
            insert_query = """
            INSERT INTO Video_details (Video_Id, channel_id, channel_name, Title, Tags, Thumbnail, Description, Published_Date, Duration, Views, Likes, Comments, Favourite_Count, Definition, Caption_Status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Execute the INSERT query for each video detail
            for video in video_details:
                try:
                    published_date = convert_iso_to_mysql_datetime(video['Published_Date'])
                    
                    # Convert Views, Likes, and Comments to integers
                    views = int(video['Views']) if video['Views'] is not None else None
                    likes = int(video['Likes']) if video['Likes'] is not None else None
                    comments = int(video['Comments']) if video['Comments'] is not None else None

                    cursor.execute(insert_query, (
                        video['Video_Id'],
                        video['channel_id'],
                        video['channel_name'],
                        video['Title'],
                        video['Tags'],
                        video['Thumbnail'],
                        video['Description'],
                        published_date,
                        video['Duration'],
                        views,
                        likes,
                        comments,
                        video['Favourite_Count'],
                        video['Definition'],
                        video['Caption_Status']
                    ))
                    print("Inserted Video Details successfully!")
                except Error as e:
                    print(f"Error inserting video {video['Video_Id']}: {e}")

            # Commit the transaction
            connection.commit()

    except Error as e:
        # Handle errors
        print("Error while connecting to MySQL:", e)

    finally:
        # Close the database connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
create_and_insert_video_details()





def create_and_insert_comment_details():
    try:
        # Connect to MySQL server
        connection = db.connect(
            host='localhost',
            database='youtube',
            user='root',
            password='root'
        )

        if connection.is_connected():
            # Create a cursor object
            cursor = connection.cursor()

            # Define your table creation SQL query
            create_table_query = """
            CREATE TABLE IF NOT EXISTS Comment_details (
            comment_Id varchar(255) primary key,
            video_id varchar(255) references Video_details(Video_Id),
            Comment_Text LONGTEXT,
            Comment_Author varchar(200),
            Comment_Published Timestamp
           )
            """

            # Execute the table creation query
            cursor.execute(create_table_query)
            print("Table created successfully!")

            # Define the INSERT query
            insert_query = """
             INSERT INTO Comment_details (Comment_Id,video_id,Comment_Text, Comment_Author,Comment_Published)
             VALUES (%s,%s,%s,%s,%s)
             """

            # Execute the INSERT query for each comment detail
            for comment in commment_details:
                try:
                     comment_published = convert_iso_to_mysql_datetime(comment['Comment_Published'])
                    
                     cursor.execute(insert_query,(comment['Comment_Id'],comment['Video_Id'],comment['Comment_Text'], comment['Comment_Author'],comment_published))
                     print("Inserted Comment Details successfully!")

                except Error as e:
                     print(f"Error inserting comment {comment['Comment_Id']}: {e}")

            # Commit the transaction
            connection.commit()

    except Error as e:
        # Handle errors
        print("Error while connecting to MySQL:", e)

    finally:
        # Close the database connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
create_and_insert_comment_details()







