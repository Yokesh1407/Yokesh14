from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#MY API
def My_api():
    api_key = 'AIzaSyAeDU-UbmFdxu31QYQVfFsCfsLfxeAFK-A' #my API Key youtube
    youtube = build('youtube', 'v3', developerKey=api_key) 
    return youtube
    
youtube = My_api()

#CHANNEL API'S

#channel_id = 'UCTxfYLmM82aMCovQexkRxFQ'  # channel ID : CHERRY_VLOGS
#channel_id = 'UCJcCB-QYPIBcbKcBQOTwhiA' #VJ siddhu
#channel_id = 'UCYqXh1HzJSYYYmbaoK4veDw' #HOBBY EXPLORER
#channel_id = 'UCvyZS6W6zMJCZBVzF-Ei6sw' #a2d
#channel_id = 'UCi3o8sgPl4-Yt501ShuiEgA' #finally

#GETTING CHANNEL DETAILS USING DEF FUNC FROM YOUTUBE
def get_channel(channel_id):
    channel_request = youtube.channels().list( #function to get api data from youtube using channelid and lists
    id=channel_id,
    part='snippet,statistics,contentDetails'
)
    channel_response = channel_request.execute() #execute all the data of the channel in api

    for i in channel_response["items"]:
        channel_informations = {
            'channel_name' : i['snippet']['title'],
            'channel_id' : i['id'],
            'Subcriber_count' : i['statistics']['subscriberCount'],
            'Channel_views' : i['statistics']['viewCount'],
            'Total_Videos':i["statistics"]["videoCount"],
            'Channel_description' : i['snippet']['description'],
            'Playlist_id' : i['contentDetails']['relatedPlaylists']['uploads']
         }

        return channel_informations

#channel_details = get_channel(channel_id) 
#print(channel_details)

#GETTING PLAYLIST IDS DATA FROM YOUTUBE 
def get_videoIDs(channel_id):
    videos_IDlists =[]
    playlist_request = youtube.channels().list( #function to get api data from youtube using channelid and contentdetails
        id=channel_id,
        part='contentDetails') #inside contentdetail only playlist is present

    playlist_response = playlist_request.execute()

    playlist_id = playlist_response['items'][0]['contentDetails']['relatedPlaylists']['uploads'] # 'UUJcCB-QYPIBcbKcBQOTwhiA' - VJ SIDDHU playlist id

    next_page_token =None

    #getting all the video ids

    while True:
        playlistitem_request = youtube.playlistItems().list(
            part="snippet",                      #inside snippet only we have playlistId so we are mapping with our playlist_id 
            playlistId=playlist_id,  
            maxResults = 50,
            #The maxResults parameter specifies the maximum number of items that should be returned in the result set. Acceptable values are 0 to 50, inclusive. The default value is 5.
            pageToken = next_page_token   #pagetoken will retreive the next page data used to navigate next page data taking maxcounts eg 50,100,150
        )
        playlistitem_response = playlistitem_request.execute()

        for i in range(len(playlistitem_response['items'])): #len(playlistitem_response['items']) value will be 50 and next cycle next 50 because we gave maxResult as 50 using it to get all video ids
            videos_IDlists.append(playlistitem_response['items'][i]['snippet']['resourceId']['videoId']) #inserting into empty list
        next_page_token = playlistitem_response.get('nextPageToken') #get function will return if we have value if we don't have values it will return null
    
        if next_page_token is None: 
            break
    #playlistitem_response['items'][0]['snippet']['resourceId']['videoId'] this will give videoId's data this data is appended in empty list
    return videos_IDlists
    #print(playlistitem_response) #will show the playlist id data's
        #print(videos_IDlists) will show all the video ids
        #print(len(videos_IDlists)) will show count of all the video ids

#videos_ids_all = get_videoIDs(channel_id)
#len(videos_ids_all)

#GETTING VIDEO INFORMATION USING VIDEO IDS 
def get_video_details(videos_ids_all):
    video_data=[]
    for video_id in videos_ids_all:
        request=youtube.videos().list(
        part="snippet,ContentDetails,statistics",
        id=video_id
    )
        video_response=request.execute()

        for item in video_response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'), #using get because the sometimes it won't be present
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description'),
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount'),
                Likes=item['statistics'].get('likeCount'),
                Comments=item['statistics'].get('commentCount'),
                Favorite_Count=item['statistics']['favoriteCount'],
                Definition=item['contentDetails']['definition'],
                Caption_Status=item['contentDetails']['caption']
        )
        video_data.append(data)
    return video_data

#video_details = get_video_details(videos_ids_all)
#len(video_details)

#GETTING COMMENT INFO USING VIDEO INFORMATION
def get_comment_info(videos_ids_all): #videos_ids_all has video_id dat from that only we are taking comments details so we are using it as arguments
    comment_data=[]
    try:
            for video_id in videos_ids_all:
                    comment_request=youtube.commentThreads().list(
                            part="snippet",
                            videoId=video_id,
                            maxResults=50
                            
                    )
                    response=comment_request.execute()


                    for item in response["items"]:
                            comment=dict(comment_id = item['snippet']['topLevelComment']['id'],
                                    video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                                    comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    Author_name = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    comment_published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                            
                            comment_data.append(comment)
                    
    except:
            pass
            return comment_data
    
#Comment_details1 = get_comment_info(videos_ids_all)

#GETTING PLAYLIST DETAILS FROM YOUTUBE USING CHANNEL ID:

def get_playlist_details(channel_id):
    next_page_token=None
    playlist_data=[]
    while True:
            playlistrequest=youtube.playlists().list(
                    part='snippet,contentDetails',
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token
            )
            playlistresponse=playlistrequest.execute()

            for item in playlistresponse['items']:
                    data=dict(Playlist_Id=item['id'],
                            Title=item['snippet']['title'],
                            Channel_Id=item['snippet']['channelId'],
                            Channel_Name=item['snippet']['channelTitle'],
                            PublishedAt=item['snippet']['publishedAt'],
                            Video_Count=item['contentDetails']['itemCount'])
                    playlist_data.append(data)

            next_page_token=playlistresponse.get('nextPageToken')
            if next_page_token is None:
                    break
    return playlist_data

#playlist = get_playlist_details(channel_id)


#MONOGODB CONNECTION:
client = pymongo.MongoClient("mongodb+srv://yokesh:root1234@yokesh.x3ibwxy.mongodb.net/")
db=client["YOUTUBE_DATA_NEW"] # created YOUTUBE_DATA in mongodb.

#INSERTING ALL THE DATA'S IN MONGODB
def Allchannel_details(channel_id):
    get_channels = get_channel(channel_id)
    videoID = get_videoIDs(channel_id)
    videodetails = get_video_details(videoID) # we are giving videoid as input to get video details 
    comment_id = get_comment_info(videoID) # we are giving videoid as input to get comment info
    playlistdetails = get_playlist_details(channel_id)
    
    collect=db["AllChannel_Details"]
    collect.insert_one({"Channel":get_channels,"Playlist":playlistdetails,"Comment":comment_id,"VideoDetails":videodetails}) #inserting these data's in monogodb as json data (":" is said to scope) 

    return "Uploaded Successfully"

#ins = Allchannel_details(channel_id)

#POSTGRESQL 
#CREATING TABLE FOR CHANNEL:
def channel_table(single_channel_name):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Root@123",
                            database="youtube",
                            port="5433")
    cursor = mydb.cursor()

    
    create_query = '''create table if not exists channel(channel_name varchar(255),
                                                            channel_id varchar(255) primary key,
                                                            Subcriber_count bigint,
                                                            Channel_views bigint,
                                                            Total_Videos int,
                                                            Channel_description text,
                                                            Playlist_id varchar(100))'''
    
    cursor.execute(create_query) #EXECUTE command prepares and runs commands dynamically
    mydb.commit() 


    print("Channel tables are already created")

    #extracted channel data from mongodb and changed the data into dataframe(table)
    single_channel_detail=[]
    db=client["YOUTUBE_DATA_NEW"]
    collect=db["AllChannel_Details"]
    for chan_data in collect.find({"Channel.channel_name": single_channel_name},{"_id":0}):
            single_channel_detail.append(chan_data["Channel"])
    df_single_channel_detail=pd.DataFrame(single_channel_detail)


    #extracted channeldata from mongodb inserted in prostgresql
    for index,rows in df_single_channel_detail.iterrows():
        insert_query = '''insert into channel(channel_name ,
                                                channel_id ,
                                                Subcriber_count ,
                                                Channel_views ,
                                                Total_Videos ,
                                                Channel_description ,
                                                Playlist_id )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        
        values =(rows["channel_name"],
                rows["channel_id"],
                rows["Subcriber_count"],
                rows["Channel_views"],
                rows["Total_Videos"],
                rows["Channel_description"],
                rows["Playlist_id"])
        
        
        cursor.execute(insert_query,values) #execute the data of insert_queries and values
        mydb.commit()

        

#CREATING TABLE FOR PLAYLISTS
def Playlists_table(single_channel_name):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Root@123",
                            database="youtube",
                            port="5433")
    cursor = mydb.cursor()

    create_query = '''create table if not exists Playlists( Playlist_Id varchar(255) primary key,
                                                            Channel_Id varchar(255),
                                                            Channel_Name varchar(255)
                                                            )'''

    cursor.execute(create_query) #EXECUTE command prepares and runs commands dynamically
    mydb.commit() 

    #creating playlists
    single_playlist_detail=[]
    db=client["YOUTUBE_DATA_NEW"]
    collect=db["AllChannel_Details"]
    for play_data in collect.find({"Channel.channel_name": single_channel_name},{"_id":0}):
            single_playlist_detail.append(play_data["Playlist"])
    df_single_playlist_detail=pd.DataFrame(single_playlist_detail[0]) #while giving [0] the data columns will become title of the tables and the rows will become values

    #extracted playlistdata from mongodb inserting in prostgresql
    for index,rows in df_single_playlist_detail.iterrows():
        insert_query = '''insert into Playlists(Playlist_Id,
                                                Channel_Id,
                                                Channel_Name)
                                                
                                                values(%s,%s,%s)'''
        
        values =(rows["Playlist_Id"],
                rows["Channel_Id"],
                rows["Channel_Name"],
                )
        
        
        cursor.execute(insert_query,values) #execute the data of insert_queries and values
        mydb.commit()

    


#CREATING TABLE FOR VIDEOS
def video_table(single_channel_name):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Root@123",
                            database="youtube",
                            port="5433")
    cursor = mydb.cursor()

   
    create_query ='''create table if not exists videos(Channel_Name varchar(100),
                                                Channel_Id varchar(100),
                                                Video_Id varchar(30) primary key,
                                                Title varchar(150),
                                                Tags text,
                                                Thumbnail varchar(200),
                                                Description text,
                                                Published_Date timestamp,
                                                Duration interval,
                                                Views bigint,
                                                Likes bigint,
                                                Comments int,
                                                Favorite_Count int,
                                                Definition varchar(10),
                                                Caption_Status varchar(50)
                                                            )'''
    
    cursor.execute(create_query) #EXECUTE command prepares and runs commands dynamically
    mydb.commit() 


    #extracted video data from mongodb and changed the data into dataframe(table)
    single_video_detail=[]
    db=client["YOUTUBE_DATA_NEW"]
    collect=db["AllChannel_Details"]
    for play_data in collect.find({"Channel.channel_name": single_channel_name},{"_id":0}):
            single_video_detail.append(play_data["VideoDetails"])

    df_single_video_detail = pd.DataFrame(single_video_detail[0]) #while giving [0] the data columns will become title of the tables and the rows will become values



    for index,row in df_single_video_detail.iterrows():
            insert_query='''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
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
                                                    Caption_Status
                                                )
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        

            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
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
                    row['Caption_Status']
                    )

            
            cursor.execute(insert_query,values)
            mydb.commit()

#CREATING TABLE FOR COMMENTS
#Table video 
def comment_table(single_channel_name):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Root@123",
                            database="youtube",
                            port="5433")
    cursor = mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    try:
        create_query ='''create table if not exists comments(comment_id varchar(100) primary key,
                                                    video_Id varchar(100),
                                                    comment_text text,
                                                    Author_name varchar(150),
                                                    comment_published timestamp
                                                                )'''
        
        cursor.execute(create_query) #EXECUTE command prepares and runs commands dynamically
        mydb.commit() 

    except:
        print("comment tables are already created")

    #extracted comment data from mongodb and changed the data into dataframe(table)
    single_comment_detail=[]
    db=client["YOUTUBE_DATA_NEW"]
    collect=db["AllChannel_Details"]
    for play_data in collect.find({"Channel.channel_name": single_channel_name},{"_id":0}):
            single_comment_detail.append(play_data["Comment"])

    df_single_comment_detail = pd.DataFrame(single_comment_detail[0]) #while giving [0] the data columns will become title of the tables and the rows will become values

    for index,row in df_single_comment_detail.iterrows():
            insert_query='''insert into comments(comment_id,
                                                    video_Id,
                                                    comment_text,
                                                    Author_name,
                                                    comment_published
                                                )    
                                                values(%s,%s,%s,%s,%s)'''
        

            values=(row['comment_id'],
                    row['video_Id'],
                    row['comment_text'],
                    row['Author_name'],
                    row['comment_published'],
                    
                    )

            cursor.execute(insert_query,values)
            mydb.commit()

#CREATING A FUNCTION TO CALL ALL THE TABLES:
def Sql_Tables(Single_channel):
    channel_table(Single_channel),
    Playlists_table(Single_channel),
    video_table(Single_channel),
    comment_table(Single_channel)
    return "Tables are created successfully"

#CREATING A DATAFRAME FUNC TO BE DISPLAYED IN SQL:
def show_channelDetails():
    Ch_list=[]
    db=client["YOUTUBE_DATA_NEW"]
    collect=db["AllChannel_Details"]
    for chan_data in collect.find({},{"_id":0,"Channel":1}):
        Ch_list.append(chan_data["Channel"])
    df=st.dataframe(Ch_list)

    return df

def show_playlistDetails():        
    pl_list=[]
    db=client["YOUTUBE_DATA_NEW"]
    collect=db["AllChannel_Details"]
    for play_data in collect.find({},{"_id":0,"Playlist":1}):
        for i in range(len(play_data["Playlist"])):
            pl_list.append(play_data["Playlist"][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videoDetails():        
    Vid_list=[]
    db=client["YOUTUBE_DATA_NEW"]
    collect=db["AllChannel_Details"]
    for Vid_data in collect.find({},{"_id":0,"VideoDetails":1}):
        for i in range(len(Vid_data["VideoDetails"])):
            Vid_list.append(Vid_data["VideoDetails"][i])
    df2=st.dataframe(Vid_list)

    return df2

def show_commentDetails():    
    com_list=[]
    db=client["YOUTUBE_DATA_NEW"]
    coll1=db["AllChannel_Details"]
    for com_data in coll1.find({},{"_id":0,"Comment":1}):
        if com_data["Comment"] is not None:
            for i in range(len(com_data["Comment"])):
                com_list.append(com_data["Comment"][i])
    df3=st.dataframe(com_list)

    return df3

#STEAMLIT
#steamlit - UI 

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skills")
    st.caption("Python scripting")
    st.caption("Data collection")
    st.caption("MONGODB")
    st.caption("Data integration")
    st.caption("Data management using mongodb and sql")

channel_id = st.text_input("Enter the Channel Id")

if st.button("Collect and store data"): #creating button youtube->mongodb data is collected to avoid duplicates in mongodb we are using button
    chan_ids=[]
    db=client["YOUTUBE_DATA_NEW"]
    collect=db["AllChannel_Details"]
    for chan_data in collect.find({},{"_id":0,"Channel":1}):
        chan_ids.append(chan_data["Channel"]["channel_id"])

    if channel_id in chan_ids:
        st.success("Channel details of the given Channel Id is already created")
    else:
        insert=Allchannel_details(channel_id)
        st.success(insert)

#creating select box to select the channels and after selecting the channel get moves to sql.
channel_lists=[]
db=client["YOUTUBE_DATA_NEW"]
coll1=db["AllChannel_Details"]
for chan in coll1.find({},{"_id":0,"Channel":1}):
        channel_lists.append(chan["Channel"]["channel_name"])

uniqueChannels = st.selectbox("Select the channels",channel_lists)

#moving channel data into sql
if st.button("Move to SQL"):
    Table = Sql_Tables(uniqueChannels) #here we are giving arguments as uniquechannels because only the particular channel only loaded in sql.
    st.success("Table are created successfully")

show_table=st.radio("SELECT THE TABLE TO BE VIEWED",("CHANNEL","PLAYLISTS","VIDEOS","COMMENTS")) #CREATING A RADIO BUTTON TO DISPLAY

if show_table == "CHANNEL":
    show_channelDetails()
elif show_table == "PLAYLISTS":
    show_playlistDetails()
elif show_table == "VIDEOS":
    show_videoDetails()
elif show_table == "COMMENTS":
    show_commentDetails()

#QUESTION TO BE ANSWERED;
mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Root@123",
                        database="youtube",
                        port="5433")
cursor = mydb.cursor()

Questions = st.selectbox("Select your Question",("1.All the videos and their corresponding channels",
                                                 "2.channels have the most number of videos and count",
                                                 "3.Top 10 most viewed videos",
                                                 "4.comments in each videos",
                                                 "5.Videos with highest likes",
                                                 "6.likes of all videos",
                                                 "7.views of each channel",
                                                 "8.videos published in the year of 2022",
                                                 "9.average duration of all videos in each channel",
                                                 "10.videos with highest number of comments"
                                                 ))
#The execute() method is used to execute the SELECT query.
#The fetchall() method retrieves all the rows returned by the query and stores them in the rows variable.
if Questions=="1.All the videos and their corresponding channels":
     query1='''Select title as videos,channel_name as channelname from videos'''
     cursor.execute(query1)
     mydb.commit()
     t1=cursor.fetchall()
     df=pd.DataFrame(t1,columns=["video title","channel name"])
     st.write(df)

elif Questions=="2.channels have the most number of videos and count":
     query2='''select channel_name,total_videos as No_of_videos from channel order by total_videos desc'''
     cursor.execute(query2)
     mydb.commit()
     t2=cursor.fetchall()
     df1=pd.DataFrame(t2,columns=["channel_name","No of videos"])
     st.write(df1)

elif Questions=="3.Top 10 most viewed videos":
     query3='''select channel_name,title as Title,views as Views from videos order by views desc limit 10'''
     cursor.execute(query3)
     mydb.commit()
     t3=cursor.fetchall()
     df2=pd.DataFrame(t3,columns=["channel_name","Title","Top 10 Views"])
     st.write(df2)

elif Questions =="4.comments in each videos":
     query4='''select title as video_title,comments from videos where comments is not null'''
     cursor.execute(query4)
     mydb.commit()
     t4=cursor.fetchall()
     df3=pd.DataFrame(t4,columns=["Video_Title","Comments"])
     st.write(df3)
    
elif Questions=="5.Videos with highest likes":
     query5='''select channel_name,title as Video_title,likes as video_likes from videos where likes is not null order by likes desc'''
     cursor.execute(query5)
     mydb.commit()
     t5=cursor.fetchall()
     df4=pd.DataFrame(t5,columns=["channel_name","Video_Title","Video_likes"])
     st.write(df4)

elif Questions=="6.likes of all videos":
     query6='''select title as Video_title,likes as video_likes from videos'''
     cursor.execute(query6)
     mydb.commit()
     t6=cursor.fetchall()
     df5=pd.DataFrame(t6,columns=["Video_Title","Video_likes"])
     st.write(df5)

elif Questions=="7.views of each channel":
     query7='''select channel_name,channel_views from channel'''
     cursor.execute(query7)
     mydb.commit()
     t7=cursor.fetchall()
     df6=pd.DataFrame(t7,columns=["channel_name","channel_views"])
     st.write(df6)

elif Questions=="8.videos published in the year of 2022":
     query8='''select channel_name,title as Channel_Title,published_date from videos where extract(year from published_date)=2022'''
     cursor.execute(query8)
     mydb.commit()
     t8=cursor.fetchall()
     df7=pd.DataFrame(t8,columns=["channel_name","Channel_Title","published_date"])
     st.write(df7)

elif Questions=="9.average duration of all videos in each channel":
        query9='''select channel_name as channelName,AVG(duration) as Avergaeduration from videos group by channel_name'''
        cursor.execute(query9)
        mydb.commit()
        t9=cursor.fetchall()
        df8=pd.DataFrame(t9,columns=["channel_name","AverageDuration"])

        T9=[]
        for index,row in df8.iterrows():
            channel_title=row["channel_name"]
            average_duration=row["AverageDuration"]
            average_duration_Str = str(average_duration)
            T9.append(dict(channeltitle=channel_title,avg=average_duration_Str))
            df10= pd.DataFrame(T9)
          
        st.write(df10)

elif Questions=="10.videos with highest number of comments":
    query10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df9=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df9)