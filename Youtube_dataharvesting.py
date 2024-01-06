# Youtube Data harvesting and Warehousing using SQL,MONGODB and StreamLit
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import json
import streamlit as st


# API Key Connection
def Api_Key_Connection():
    Api_Id ="AIzaSyBJGs8rTeDlCL6cgvEZaraE6yHptWg5FtA"

    Api_service_name = "youtube"
    Api_version = "v3"
    youtube = build(Api_service_name,Api_version,developerKey = Api_Id)
    return youtube

youtube = Api_Key_Connection()
    
#get channel information
def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(
                    Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    )
        return data
    
# get Playlist ids
def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                  'Title':item['snippet']['title'],
                  'ChannelId':item['snippet']['channelId'],
                  'ChannelName':item['snippet']['channelTitle'],
                   'PublishedAt':item['snippet']['publishedAt'],
                   'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data

# GET Video Id
def get_video_ids(channel_id):
 video_ids=[]
 response=youtube.channels().list(id = channel_id,
                                       part ="contentDetails").execute()
 Playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

 next_page_token = None


 while True:
     response1=youtube.playlistItems().list(
                                    part ="snippet",
                                    playlistId=Playlist_Id,
                                    maxResults=50,
                                    pageToken=next_page_token).execute()
     for i in range(len(response1["items"])):                                 
       video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])    
     next_page_token=response1.get("nextPageToken")

     if next_page_token is None:
        break
 return video_ids

#Get Video Information  
def get_video_info(video_ids):
    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            data = dict(
               Channel_Name=item['snippet']['channelTitle'],
               Channel_Id=item['snippet']['channelId'],
               Video_ID=item['id'],
               Title=item['snippet']['title'],
               Tags=item['snippet'].get('tags',['na']),
               Thumbnail=item['snippet']['thumbnails']['default']['url'],
               Description=item['snippet'].get('description',['na']),
               Published_Date=item['snippet']['publishedAt'],
               Duration=item['contentDetails']['duration'],
               Views=item['statistics'].get('viewCount',0),
               Likes = item['statistics'].get('likeCount',0),
               Comments=item['statistics'].get('commentCount',0),
               Favorite_Count=item['statistics']['favoriteCount'],
               Definition=item['contentDetails']['definition'],
               Caption_status=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data


# Get Comment Information
def get_Comment_information(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for item in response["items"]:
                data = dict(
                    Comment_id=item['snippet']['topLevelComment']['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published_date=item['snippet']['topLevelComment']['snippet']['publishedAt'],
                    LikeCount=item['snippet']['topLevelComment']['snippet']['likeCount']
                )
                Comment_data.append(data)
    except:
         pass
    return Comment_data


#MongoDB Connection Establishment
client = pymongo.MongoClient("mongodb+srv://ramanaathan1:rAMANAA5882@cluster0.mzvob39.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]


# Upload to MongoDB
def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_info(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    comm_details=get_Comment_information(vi_ids)

    collec1=db["channel_details"]
    collec1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,"comment_information":comm_details})

    return "Upload Completed Successfully"

#CHANNEL DETAILS
#Pawan Lalwani         "UC5fs7PookxGfDPTo-RU0ReQ"
#Mr Gk                 "UC5cY198GU1MQMIPJgMkCJ_Q"
#Data Science in tamil "UCTCMjShTpZg96cXloCO9q1w"
#ScientificThamizhans  "UCfbWU8xoxvzDSTQsqLNnVog"
#Mr T pokemon          "UCU3wULlj7uCYKjZ32lbtazQ"
#Guri Bolte            "UC7XZytvp1zBEMvnHm5lmwOA"
#Shridhar V            "UCKQeGTsgUcO8eFoeSD-39rw"
#TanyaKhanijow         "UCGeGhS_akOxBWQcSmje6B-w"
#KingsleyMusicLessons  "UCv0kbxb0quwSawbx6nQekdg"
#electrophoenixzara    "UC2TcWTdvMIcuSbHIJMHjPRA"


# Table Creation of channel:

def channels_table():
   # PostgreSQL connection
   mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="onssnm1972",
                            database="Youtube_data",
                            port="5432")
   cursor = mydb.cursor()
   
   # Drop existing table
   drop_query = '''DROP TABLE IF EXISTS CHANNELS'''
   cursor.execute(drop_query)
   mydb.commit()
   
   # Create new table
   create_query = '''CREATE TABLE IF NOT EXISTS CHANNELS (Channel_Name VARCHAR(100),
                                                          Channel_Id VARCHAR(80) PRIMARY KEY,
                                                          Subscription_Count BIGINT,
                                                          Views  BIGINT,
                                                          Total_Videos INT ,
                                                          Channel_Description Text ,
                                                          Playlist_Id varchar(50)
                                                            )'''
   cursor.execute(create_query)
   mydb.commit()
       
   # MongoDB connection
   db = client["Youtube_data"]
   colle1 = db["channel_details"]

   # Retrieve data from MongoDB
   cl_list=[]
   db = client["Youtube_data"]
   colle1 = db["channel_details"]
   for cl_data in colle1.find({}, {"_id": 0, "channel_information": 1}):
        cl_list.append(cl_data["channel_information"])
   df = pd.DataFrame(cl_list)

   # Insert values into PostgreSQL table
   for index, row in df.iterrows():
      insert_query = '''insert into channels (Channel_Name,
                                              Channel_Id,
                                              Subscription_Count,
                                              Views,
                                              Total_Videos,
                                              Channel_Description,
                                              Playlist_Id
                                                )
                                                VALUES(%s,%s,%s,%s,%s,%s,%s)'''
      values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscription_Count'],
            row['Views'],
            row['Total_Videos'],
            row['Channel_Description'],
            row['Playlist_Id']
        )
      try:
       cursor.execute(insert_query, values)
       mydb.commit()
       
      except:
         print("playlist values are inserted")



# create playlist table
def playlists_table():

    # PostgreSQL connection
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="onssnm1972",
        database="Youtube_data",
        port="5432"
    )
    cursor = mydb.cursor()

    # Drop existing table
    drop_query = '''DROP TABLE IF EXISTS PLAYLISTS'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create new table
    try:
        create_query ='''CREATE TABLE IF NOT EXISTS PLAYLISTS (
                        PlaylistId	 VARCHAR(100) PRIMARY KEY,
                        Title VARCHAR(100),
                        ChannelId VARCHAR(100),
                        ChannelName VARCHAR(100),
                        PublishedAt TIMESTAMP,
                        VideoCount INT
                    )'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("playlist table already created")


    # MongoDB connection
    db = client["Youtube_data"]
    colle1 = db["channel_details"]

    # Retrieve data from MongoDB
    pl_list = []
    for pl_data in colle1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)

    # Insert values into PostgreSQL table
    for index, row in df1.iterrows():
        insert_query = '''insert into playlists (
                            PlaylistId	,
                            Title,
                            ChannelId,
                            ChannelName,
                            PublishedAt,
                            VideoCount
                        )
                        VALUES(%s,%s,%s,%s,%s,%s)'''
        values = (
            row['PlaylistId'],
            row['Title'],
            row['ChannelId'],
            row['ChannelName'],
            row['PublishedAt'],
            row['VideoCount']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("playlist table values are inserted")

            
# create videos table
def videos_table():
    # PostgreSQL connection
    mydb = psycopg2.connect(host="localhost",
                user="postgres",
                password="onssnm1972",
                database= "Youtube_data",
                port = "5432"
                )
    cursor = mydb.cursor()

    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists videos(
                        Channel_Name varchar(150),
                        Channel_Id varchar(100),
                        Video_ID varchar(50) primary key, 
                        Title varchar(150), 
                        Tags text,
                        Thumbnail varchar(225),
                        Description text, 
                        Published_Date timestamp,
                        Duration interval, 
                        Views bigint, 
                        Likes bigint,
                        Comments int,
                        Favorite_Count int, 
                        Definition varchar(10), 
                        Caption_status varchar(50) 
                        )''' 
                        
        cursor.execute(create_query)             
        mydb.commit()
    except:
        st.write("Videos Table already created")

    vi_list = []
    db = client["Youtube_data"]
    colle1 = db["channel_details"]
    for vi_data in colle1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
        
    
    for index, row in df2.iterrows():
        insert_query = '''
                    INSERT INTO videos (Channel_Name,
                                        Channel_Id,
                                        Video_ID, 
                                        Title, 
                                        Tags,
                                        Thumbnail,
                                        Description, 
                                        Published_Date,
                                        Duration, 
                                        Views, 
                                        Likes,
                                        Comments,
                                        Favorite_Count, 
                                        Definition, 
                                        Caption_status 
                                      )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                '''
        values = (
                    row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_ID'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_status'])
                                
        try:    
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            st.write("videos values  inserted in the table")


# create comment table
def comment_table():
    # PostgreSQL connection
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="onssnm1972",
        database="Youtube_data",
        port="5432"
    )
    cursor = mydb.cursor()

    # Drop existing table
    drop_query = '''DROP TABLE IF EXISTS COMMENTS'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create new table
    create_query = '''CREATE TABLE IF NOT EXISTS COMMENTS (
        Comment_id VARCHAR(100) PRIMARY KEY,
        Video_Id VARCHAR(50),
        Comment_Text TEXT,
        Comment_Author VARCHAR(150),
        Comment_Published_date TIMESTAMP,
        LikeCount INT
    )'''
    cursor.execute(create_query)
    mydb.commit()

    # MongoDB connection
    db = client["Youtube_data"]
    collec1 = db["channel_details"]

    # Retrieve data from MongoDB
    com_list = []
    for com_data in collec1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    # Create DataFrame from MongoDB data
    df3 = pd.DataFrame(com_list)

    # Insert values into PostgreSQL table
    for index, row in df3.iterrows():
        insert_query = '''
            INSERT INTO comments (
                             Comment_id, 
                             Video_Id,
                             Comment_Text,
                             Comment_Author,
                             Comment_Published_date,
                             LikeCount
                           )
                           VALUES (%s, %s, %s, %s, %s, %s)
                       '''
        values = (
            row['Comment_id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_Published_date'],
            row['LikeCount']
        )
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("comment values are inserted")



def all_tables():
    channels_table()
    playlists_table()
    videos_table()
    comment_table()

    return "Tables created successfully"

def show_channels_table():
  cl_list=[]
  db = client["Youtube_data"]
  colle1 = db["channel_details"]
  for cl_data in colle1.find({}, {"_id": 0, "channel_information": 1}):
      cl_list.append(cl_data["channel_information"])
  df = st.dataframe(cl_list)

  return df

def show_playlists_table():
  pl_list = []
  db = client["Youtube_data"]
  colle1 = db["channel_details"]
  for pl_data in colle1.find({}, {"_id": 0, "playlist_information": 1}):
      for i in range(len(pl_data["playlist_information"])):
        pl_list.append(pl_data["playlist_information"][i])
  df1 = st.dataframe(pl_list)

  return df1

def show_videos_table():
  vi_list = []
  db = client["Youtube_data"]
  collec1 = db["channel_details"]
  for vi_data in collec1.find({}, {"_id": 0, "video_information": 1}):
      for i in range(len(vi_data["video_information"])):
        vi_list.append(vi_data["video_information"][i])
  df2 = st.dataframe(vi_list)

  return df2

def show_comments_table():
  com_list = []
  db = client["Youtube_data"]
  collec1 = db["channel_details"]
  for com_data in collec1.find({}, {"_id": 0, "comment_information": 1}):
      for i in range(len(com_data["comment_information"])):
         com_list.append(com_data["comment_information"][i])
  df3 = st.dataframe(com_list)

  return df3


# streamlit 
with st.sidebar:
  st.sidebar.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
  st.sidebar.header("Project benefits")
  st.sidebar.caption("Explore the YouTube API: Learn how to access and fetch data from YouTube's extensive API...")

st.sidebar.subheader("Data Collection")
new_channel_id = st.sidebar.text_input("ENTER NEW CHANNEL ID")
if st.sidebar.button("COLLECT AND STORE DATA IN MONGODB"):
    ch_ids = []
    db = client["Youtube_data"]
    collec1 = db["channel_details"]
    for ch_data in collec1.find({}, {"_id": 0, "channel_information": 1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if new_channel_id in ch_ids:
        st.sidebar.warning("CHANNEL DETAILS OF THE GIVEN CHANNEL ID ALREADY EXISTS ")
    else:
        insert = channel_details(new_channel_id)
        st.sidebar.success(insert)

st.sidebar.subheader("Table Migration")
if st.sidebar.button("MIGRATE TO POSTGRESQL"):
    Table = all_tables()
    st.sidebar.success(Table)

# Show output as dataframes in streamlit
show_table = st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table =="CHANNELS":
   show_channels_table()

elif show_table =="PLAYLISTS":
   show_playlists_table()

elif show_table == "VIDEOS":
   show_videos_table()

elif show_table =="COMMENTS":
   show_comments_table()


# PostgreSQL connection
mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="onssnm1972",
        database="Youtube_data",
        port="5432"
    )
cursor = mydb.cursor()



Question = st.selectbox("EXECUTE QUESTION",    ("1.What are the names of all the videos and their corresponding channels?",
                                                "2.Which channels have the most number of videos, and how many videos do they have?",
                                                "3.What are the top 10 most viewed videos and their respective channels?",
                                                "4.How many comments were made on each video, and what are their corresponding video names?",
                                                "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                                "8.What are the names of all the channels that have published videos in the year 2022?",
                                                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                "10.Which videos have the highest number of comments, and what are their corresponding channel names?" ))




if Question == '1.What are the names of all the videos and their corresponding channels?':
   query1 ='''select channel_name as channelname ,title as Videotitle from videos '''
   cursor.execute(query1)
   mydb.commit()
   t1=cursor.fetchall()
   st.write(pd.DataFrame(t1,columns=["CHANNEL NAME","VIDEO TITLE"]))
   

elif Question == '2.Which channels have the most number of videos, and how many videos do they have?':
   query2 ='''select channel_name as channelname,total_videos as no_of_videos from channels 
           order by total_videos desc'''
   cursor.execute(query2)
   mydb.commit()
   t2=cursor.fetchall()
   st.write(pd.DataFrame(t2,columns=["CHANNEL NAME","NO OF VIDEOS"]))
   

elif Question ==  '3.What are the top 10 most viewed videos and their respective channels?':
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["VIEWS","CHANNEL NAME","VIDEO TITLE"]))

elif Question == '4.How many comments were made on each video, and what are their corresponding video names?':
    query4 = "select Comments as Nocomments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["NO COMMENTS", "VIDEO TITLE"]))
   

elif Question == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    t5_df = pd.DataFrame(t5, columns=["VIDEO TITLE", "CHANNEL NAME", "LIKE COUNT"])
    t5_df['LIKE COUNT'] = t5_df['LIKE COUNT'].astype(bytes)
    # Display the DataFrame in Streamlit
    st.write(t5_df)
   

elif Question == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["LIKE COUNT","VIDEO TITLE"]))
    

elif Question == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["CHANNEL NAME","TOTAL VIEWS"]))
   

elif Question == '8.What are the names of all the channels that have published videos in the year 2022?':
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["NAME", "VIDEO PUBLISHED ON", "CHANNELNAME"]))


elif Question == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['CHANNELTITLE', 'AVERAGEDURATION'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['CHANNELTITLE']
        average_duration = row['AVERAGEDURATION']
        average_duration_str = str(average_duration)
        T9.append({"'CHANNELTITLE'": channel_title ,  "AVERAGEDURATION": average_duration_str})
    st.write(pd.DataFrame(T9))
    

elif Question == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
   query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
   cursor.execute(query10)
   mydb.commit()
   t10=cursor.fetchall()
   st.write(pd.DataFrame(t10, columns=['VIDEO TITLE', 'CHANNEL NAME', 'NO OF COMMENTS']))








    










