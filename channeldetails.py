import streamlit as st
import pandas as pd
import googleapiclient.discovery
import mysql.connector
from mysql.connector import Error

connection = mysql.connector.connect(
            host='localhost',
            database='youtube',
            user='root',
            password='root'
 )
cursor = connection.cursor()

option = st.selectbox(
   "You can select any Questions",
   ("What are the names of all the videos and their corresponding channels?", 
    "Which channels have the most number of videos, and how many videos do they have?",
    "What are the top 10 most viewed videos and their respective channels?",
      "How many comments were made on each video, and what are their corresponding video names?"
      ,"Which videos have the highest number of likes, and what are their corresponding channel names?",
      "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
      "What is the total number of views for each channel, and what are their corresponding channel names?",
      "What are the names of all the channels that have published videos in the year 2022?",
      "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
      "Which videos have the highest number of comments, and what are their corresponding channel names?"),
   index=None,
   placeholder="Select contact method...",
)
st.write("You selected:", option)

if option=="What are the names of all the videos and their corresponding channels?":
    Query1='''select channel_name as channelName,Title as videoName from video_details'''
    cursor.execute(Query1)
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["Channel Name","Video Name"])
    st.write(df1)
elif option=="Which channels have the most number of videos, and how many videos do they have?":
    Query2='''Select channel_name,count(*) as highestcount from video_details group by channel_Name order by HighestCount desc Limit 1'''
    cursor.execute(Query2)
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel Name","highestcount"])
    st.write(df2)
elif option=="What are the top 10 most viewed videos and their respective channels?":
    Query3='''select title ,channel_name from video_details order by views desc limit 10'''
    cursor.execute(Query3)
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["Video Name","Channel Name"])
    st.write(df3)
elif option=="How many comments were made on each video, and what are their corresponding video names?":
    Query4='''Select VideoName,CommentCount From (Select count(c.comment_id) as CommentCount,c.video_id,v.title as VideoName 
    from comment_details c inner join video_details v ON c.video_id=v.video_id group by video_id)a'''
    cursor.execute(Query4)
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["Video Name","Channel Name"])
    st.write(df4)
elif option=="Which videos have the highest number of likes, and what are their corresponding channel names?":
    Query5='''Select channel_name,Likes from (Select likes,channel_name,dense_rank() over (partition by channel_name order by likes desc) as rn from video_details) a where rn=1'''
    cursor.execute(Query5)
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Channel Name","Likes"])
    st.write(df5)
elif option=="What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    Query6='''SELECT Likes,Title as VideoName FROM video_details'''
    cursor.execute(Query6)
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["Likes","VideoName"])
    st.write(df6)
elif option=="What is the total number of views for each channel, and what are their corresponding channel names?":
    Query7='''SELECT Channel_Views,Channel_Name FROM channel_details'''
    cursor.execute(Query7)
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["Total Views","Channel Name"])
    st.write(df7)
elif option=="What are the names of all the channels that have published videos in the year 2022?":
    Query8='''select Distinct Channel_name from video_details where Date_format(Published_date,'%Y')="2022"'''
    cursor.execute(Query8)
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Channel Name"])
    st.write(df8)
elif option=="Which videos have the highest number of comments, and what are their corresponding channel names?":
    Query9='''Select Video_Name,Channel_Name from (Select dense_rank() over (order by comment_count desc) as rn ,comment_count,video_name,channel_name from (select count(*) as comment_count,c.video_id,v.title as video_name ,v.channel_name from comment_details c inner join video_details v on c.video_id=v.video_id group by c.video_id order by 1 desc) a)b where rn=1'''
    cursor.execute(Query9)
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["Video Name","Channel Name"])
    st.write(df9)
    