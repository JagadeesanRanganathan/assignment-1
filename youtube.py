#[importing the necessary libraries]
# [Youtube API libraries]
import googleapiclient.discovery
from googleapiclient.discovery import build
# [pandas, numpy]
import pandas as pd
import numpy as np
# [File handling libraries]
import json
import re
from datetime import datetime
import time
import isodate

# [MongoDB]
from pymongo import MongoClient
import pymongo as pg

# [SQL libraries]
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql

#[streamlit libraries]
import streamlit as st
import matplotlib.pyplot as plt


# ==============================================         /   /   DASHBOARD   /   /         ================================================== #
st.set_page_config(layout='wide')

# Title
st.title(':red[Youtube Data Harvesting]')

# Data collection zone
col1, col2 = st.columns(2)

with col1:
    st.header(':violet[Data collection zone]')
    channel_ids = st.text_input('**Enter the channel_id**') 
    Get_data = st.button('**Get data and stored**')

    # Define Session state to Get data button
    if "Get_state" not in st.session_state:
        st.session_state.Get_state = False
    if Get_data or st.session_state.Get_state:
        st.session_state.Get_state = True
        
        # Building connection with youtube api
        api_service_name = 'youtube'
        api_version = 'v3'
        api_key = 'st.text_input'
        youtube = build(api_service_name,api_version,developerKey =api_key)
        
        # Function to get channel details:
        def get_channel_stats(youtube,channel_ids):
            channel_data = []
            request = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= ','.join(channel_ids))
            response=request.execute()
    

            for i in range(len(response['items'])):
               data = dict(
                           Channel_id = response['items'][i]['id'],
                           Channel_name = response['items'][i]['snippet']['title'],
                           Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                           Subscribers = response['items'][i]['statistics']['subscriberCount'],
                           View_count = response['items'][i]['statistics']['viewCount'],
                           Total_videos = response['items'][i]['statistics']['videoCount'],
                           Description = response['items'][i]['snippet']['description'],
                           )
            channel_data.append(data)

    
            return channel_data

        channel_data=get_channel_stats(youtube,channel_ids)

        channel_data  
        
        # Function to get playlist_id

        df = pd.DataFrame(channel_data)  

        def get_playlist_data(df):
            Playlist_ids = []

            for i in df["Playlist_id"]:
                Playlist_ids.append(i)

            return Playlist_ids

        Playlist_id=get_playlist_data(df)
        Playlist_id
        
        # Function to get video_ids
        def get_video_id(youtube,Playlist_id):
            video_id = []

            for i in Playlist_id:
                next_page_token = None
                more_pages = True

                while more_pages:
                    request = youtube.playlistItems().list(
                              part = 'contentDetails',
                              playlistId = i,
                              maxResults = 50,
                              pageToken = next_page_token)
                    response = request.execute()

                for j in response["items"]:
                    video_id.append(j["contentDetails"]["videoId"])

                next_page_token = response.get("nextPageToken")
                if next_page_token is None:
                    more_pages = False
            return video_id
        video_id= get_video_id(Playlist_id)
        video_id
        
        # Function to get video details:

        def get_video_details(youtube,video_id):
            video_stats = []
    
            for i in range(0, len(video_id), 50):
                response = youtube.videos().list(
                           part="snippet,contentDetails,statistics",
                           id=','.join(video_id[i:i+50])).execute()
                 # Function to convert duration
                for video in response["items"]:
                    published_dates = video["snippet"]["publishedAt"]
                    parsed_dates = datetime.strptime(published_dates,'%Y-%m-%dT%H:%M:%SZ')
                    format_date = parsed_dates.strftime('%Y-%m-%d')
                    duration = video["contentDetails"]["duration"]
                    Duration = isodate.parse_duration(duration)
                    video_duration = Duration.total_seconds()
            
                    for video in response['items']:
                        video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                             Channel_id = video['snippet']['channelId'],
                                             video_ids = video['id'],
                                             Title = video['snippet']['title'],
                                             Tags = video['snippet'].get('tags'),
                                             Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                             Description = video['snippet']['description'],
                                             Published_date = video['snippet']['publishedAt'],
                                             Duration = video_duration,
                                             Views = video['statistics']['viewCount'],
                                             Likes = video['statistics'].get('likeCount'),
                                             Comments = video['statistics'].get('commentCount'),
                                             Favorite_count = video['statistics']['favoriteCount'],
                                             Definition = video['contentDetails']['definition'],
                                             Caption_status = video['contentDetails']['caption']
                                            )
                        video_stats.append(video_details)
            
            return video_stats
  
            video_data=get_video_details(youtube,video_id)
            video_data
        
        # Function to get comments details:

        def get_comments(video_id):
            comments_data= []
            try:
                next_page_token = None
                for i in video_id :
                    while True:
                        request = youtube.commentThreads().list(
                                  part = "snippet,replies",
                                  videoId = i,
                                  textFormat="plainText",
                                  maxResults = 50,
                                  pageToken=next_page_token)
                        response = request.execute()
                        
                         #Function to convert duration
                        for item in response["items"]:
                            published_date= item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                            parsed_dates = datetime.strptime(published_date,'%Y-%m-%dT%H:%M:%SZ')
                            format_date = parsed_dates.strftime('%Y-%m-%d')


                            comments = dict(comment_id = item["id"],
                                            video_id = item["snippet"]["videoId"],
                                            comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                            comment_author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                            comment_published_date = format_date)
                            comments_data.append(comments) 

                        next_page_token = response.get('nextPageToken')
                        if next_page_token is None:
                            break       
            except Exception as e:
                print("An error occured",str(e))          

            return comments_data
        comment_data=get_comments(video_id)
        comment_data

        # -----------------------------------    /   MongoDB connection and store the collected data   /    ---------------------------------- #

        # user giving input
        channel_id = []

        channels = st.number_input('**Enter the number of channels**', value=1, min_value=1,max_value=10)
        for i in range(channels):
            channel_id.append(st.text_input("**Enter the ChannelID**", key=i))

        submit = st.button("Upload into MongoDB Database")

        # Connection to MongoDB

        client = pg.MongoClient("mongodb://localhost:27017")

        #create database

        my_db = client["youtube"]

        #collection creation

        channel_stats = my_db['channel_data']
        video_stats = my_db['video_data']
        comment_stats = my_db['comments_data']

        if submit:
            if channel_id:
                channel_details = get_channel_stats(channel_id)
                df = pd.DataFrame(channel_details) 
                playlist_id = get_playlist_data(df)
                video_id = get_video_id(playlist_id)
                video_details = get_video_details(video_id)
                get_comment_data = get_comments(video_id)

                with st.spinner('Wait a little bit!! '):
                    time.sleep(5)
                    st.success('Done, Data successfully')

                if channel_details:
                    channel_stats.insert_many(channel_details)
                if video_details:
                    video_stats.insert_many(video_details)
                if get_comment_data:
                    comment_stats.insert_many(get_comment_data)

                with st.spinner('Wait a little bit!! '):
                    time.sleep(5)
                    st.success('Done!, Data Successfully Uploaded')
                    st.balloons()

        #Select channel names for user input from MondoDB

        def channel_names():   
            ch_name = []
            for i in my_db.channel_data.find():
                ch_name.append(i['channel_name'])
            return ch_name
        
        st.subheader(":green[ Data inserting into MySQL...........] 🔜")

        user_input =st.multiselect("Select the channel to be inserted into MySQL Tables",options = channel_names())

        submit_next = st.button("Upload the data into MySQL")
        
        #Fetch the channel_details
        
        if submit_next:

            with st.spinner('Please wait a bit '):

                 def get_channel_details(user_input):
                    query = {"channel_name":{"$in":list(user_input)}}
                    projection = {"_id":0,"channel_id":1,"channel_name":1,"channel_views":1,"channel_subscribers":1,"total_videos":1,"playlist_id":1}
                    x = channel_stats.find(query,projection)
                    channel_table = pd.DataFrame(list(x))
                    return channel_table

                 channel_data = get_channel_details(user_input)
  
                 #Fetch the Video details:

                 def get_video_details(channel_list):
                    query = {"channel_id":{"$in":channel_list}}
                    projection = {"_id":0,"video_id":1,"channel_id":1,"channel_name":1,"video_name":1,"video_published_date":1,"video_views":1,"video_likes":1,"video_comments":1,"video_duration":1}
                    x = video_stats.find(query,projection)
                    video_table = pd.DataFrame(list(x))
                    return video_table
                 video_data = get_video_details(channel_id)

                 #Fetch the comment_details
                 def get_comment_details(video_ids):
                    query = {"video_id":{"$in":video_ids}}
                    projection = {"_id":0,"comment_id":1,"video_id":1,"comment_text":1,"comment_author":1,"comment_published_date":1}
                    x = comment_stats.find(query,projection)
                    comment_table = pd.DataFrame(list(x))
                    return comment_table

                #Fetch video_ids from mongoDb

                 video_ids = video_stats.distinct("video_id")

                 comment_data = get_comment_details(video_ids)

                 client.close()
        
        # ========================================   /     Data Migrate zone (Stored data to MySQL)    /   ========================================== #
with col2:
    # MYSQL CONNECTION

     mydb = pymysql.connect(
            host="localhost",
            port = 3306,
            user="root",
            password="jaga7375",
            database="youtube_data_warehousing")

     mycursor = mydb.cursor()
    #create an SQLAlchemy engine

     engine = create_engine('mysql+pymysql://root:jaga7375@@localhost/youtube_data_warehousing')

     #Inserting Channel data into the table using try and except method
        try:
            #inserting data
            channel_data.to_sql('channel_data', con=engine, if_exists='append', index=False, method='multi')
            print("Data inserted successfully")
        except Exception as e:
            if 'Duplicate entry' in str(e):
                print("Duplicate data found. Ignoring duplicate entries.")
            else:
                print("An error occurred:", e)
                
    #Inserting Video data into the table using try and except method

        try:
            video_data.to_sql('video_data', con=engine, if_exists='append', index=False, method='multi')
            print("Data inserted successfully")
        except Exception as e: 
            if 'Duplicate entry' in str(e):
                print("Duplicate data found. Ignoring duplicate entries.")
            else:
                print("An error occurred:", e)
        st.success("Data Uploaded Successfully")
        engine.dispose()
    #MYSQL Database connection

    mydb = pymysql.connect(
    host="localhost",
    port = 3306,
    user="root",
    password="4665",
    database="youtube_data_warehousing")

mycursor = mydb.cursor()
 


questions = st.selectbox("Select any questions given below:",
['Click the question that you would like to query',
'1. What are the names of all the videos and their corresponding channels?',
'2. Which channels have the most number of videos, and how many videos do they have?',
'3. What are the top 10 most viewed videos and their respective channels?',
'4. How many comments were made on each video, and what are their corresponding video names?',
'5. Which videos have the highest number of likes, and what are their corresponding channel names?',
'6. What is the total number of likes for each video, and what are their corresponding video names?',
'7. What is the total number of views for each channel, and what are their corresponding channel names?',
'8. What are the names of all the channels that have published videos in the year 2022?',
'9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
'10. Which videos have the highest number of comments, and what are their corresponding channel names?'])


#store the queries

if questions == '1. What are the names of all the videos and their corresponding channels?':
    query = "select distinct channel_name as Channel_name , video_name as Video_name from video_data order by cast(video_name as unsigned) asc;"
    table = pd.read_sql(query,mydb)
    st.write(table)
    st.success('Success!.. Move to next Questions⏭️',icon="✅")
    st.toast('Good job',icon="😍")
elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
    query = "select distinct channel_name as Channel_name,count(video_name) as Most_Number_of_Videos from video_data group by channel_name order by cast(Most_Number_of_Videos as unsigned) desc;"
    mycursor.execute(query)
    result = mycursor.fetchall()
    table = pd.DataFrame(result, columns=['Channel_name', 'Number_of_Videos']).reset_index(drop=True)
    table.index += 1
    st.dataframe(table)
    st.success('Success!.. Move to next Questions⏭️',icon="✅")
elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
                a = pd.read_sql("SELECT channel_Name FROM channel_data", mydb)
                channels_id = []
                for i in range(len(a)):
                    channels_id.append(a.loc[i].values[0])

                ans3 = pd.DataFrame()
                for each_channel in channels_id:
                    Q3 = f"SELECT * FROM video_data WHERE channel_name='{each_channel}' ORDER BY video_views DESC LIMIT 10"
                    ans3 = pd.concat([ans3, pd.read_sql(Q3, mydb)], ignore_index=False)
                st.write(ans3[['video_name', 'channel_name', 'video_views']])
                st.success('Success!.. Move to next Questions⏭️',icon="✅")
elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
    query = "select distinct channel_name as Channel_name , video_name as Video_name , video_comments as Comments_count from video_data order by cast(channel_name as unsigned) desc;"
    table = pd.read_sql(query,mydb)
    st.write(table)
    st.success('Success!.. Move to next Questions⏭️',icon="✅")
elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query = "select distinct channel_name as Channel_name,video_name as Video_name,video_likes as Number_of_likes from video_data order by cast(video_likes as unsigned) desc limit 10;"
    table = pd.read_sql(query,mydb)
    st.write(table)
    st.success('Success!.. Move to next Questions⏭️',icon="✅")
elif questions == '6. What is the total number of likes for each video, and what are their corresponding video names?':
    query = "select distinct video_name as Video_name,video_likes as Like_count from video_data order by cast(Like_count as unsigned) desc;"
    table = pd.read_sql(query,mydb)
    st.write(table)
    st.success('Success!.. Move to next Questions⏭️',icon="✅")
elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query = " select distinct channel_name as Channel_name , channel_views as total_number_of_views from channel_data order by cast(channel_views as unsigned) desc;"
    table = pd.read_sql(query,mydb)
    st.write(table)
    st.success('Success!.. Move to next Questions⏭️',icon="✅")
elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
    query = "select distinct channel_name as Channel_name , year(video_published_date) as published_year from video_data where year(video_published_date) = 2022;"
    table = pd.read_sql(query,mydb)
    st.write(table)
    st.success('Success!.. Move to next Questions⏭️',icon="✅")
elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query ="SELECT channel_name AS Channel_Name,AVG(video_duration) AS Average_duration FROM video_data GROUP BY channel_name ORDER BY AVG(video_duration) DESC;"
    table = pd.read_sql(query,mydb)
    st.write(table)
    st.success('Success!.. Move to next Questions⏭️',icon="✅")
elif questions =='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query = "select distinct channel_name as Channel_name , video_name as Video_name, video_comments as No_of_comments from video_data order by cast(video_comments as unsigned) desc limit 10;"
    table = pd.read_sql(query,mydb)
    st.write(table)
    st.success('Successfully Done',icon="✅") 
    st.write('Thanks for giving me this oppurtunity') 

mycursor.close()
mydb.close()
