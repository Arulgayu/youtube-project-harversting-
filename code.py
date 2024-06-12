import pandas as pd
import pymysql 
from pymysql.cursors import DictCursor
from googleapiclient.discovery import build


def API_details():
    api_key ='AIzaSyCvBbLq5jaPZdqXEXMShHgcFsP5xtVdh68'

    api_service_name = "youtube"
    api_version = "v3"

    youtube =build(api_service_name, api_version, developerKey= api_key)


    return youtube

youtube = API_details()


def get_channels_details(channel_id):
  request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id
  )

  response=request.execute()
  
  for resp in response['items']:

              Data =dict(channel_id=resp['id'],
                  channel_name=resp['snippet']['title'],
                  channel_des=resp['snippet']['description'],
                  channel_pid=resp['contentDetails']['relatedPlaylists']['uploads'],
                  channel_pat=resp['snippet']['publishedAt'],
                  channel_viewcount=resp['statistics']['viewCount'],
                  channel_sub=resp['statistics']['subscriberCount'],
                  channel_vc=resp['statistics']['videoCount']
                  
              )
                
              return Data

def get_channel_videos_ids(channel_id):
     Video_ids = []

     request=youtube.channels().list(id=channel_id,
                                        part="contentDetails"
                                        )
     response= request .execute()
     playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

     next_page_Token = None

     while True:
          request1= youtube.playlistItems().list(
                                                  part='snippet',
                                                  playlistId= playlist_Id,
                                                  maxResults=50,
                                                  pageToken=next_page_Token)
          resp1=request1.execute()

          for i in range(len(resp1['items'])):
               Video_ids.append(resp1['items'][i]['snippet']['resourceId']['videoId'])
          next_page_Token=resp1.get('next_page_Token')

          if next_page_Token is None:
                    break
     return  Video_ids

def get_video_information( video_ids):
      video_data=[]

      for video_id in video_ids :

            request = youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                                    id=video_id
                  )
            response = request.execute()
            for resp in response['items']:
                  data= dict(
                        channel_name=resp['snippet']['channelTitle'],
                        Channel_id = resp['snippet']['channelId'],
                        video_ids= resp['id'],
                        video_name =resp['snippet']['title'],
                        Tags=resp['snippet']['tags'],
                        Thumbnails=resp['snippet']['thumbnails']['default']['url'],
                        Description= resp['snippet']['description'],
                        PublishedAt= resp['snippet']['publishedAt'],
                        view_counts =resp['statistics']['viewCount'],
                        like_counts=resp['statistics']['likeCount'],
                        favorite_count=resp['statistics']['favoriteCount'],
                        comment_count=resp['statistics']['commentCount'],
                        duration=resp['contentDetails']['duration'],
                        caption_status=resp['contentDetails']['caption'])

                  video_data.append(data)
      return (video_data)





def get_comment_information(video_ids):
  comment_data=[]
  try:
      for comment_id in video_ids:

                request= youtube.commentThreads().list(
                part="snippet",
                maxResults=50,
                videoId=comment_id,                )
                response = request.execute()

                for resp in response['items']:
                        data=dict(comment_id =resp['snippet']['topLevelComment']['id'],
                                TextDisplay=resp['snippet']['topLevelComment']['snippet']['textDisplay'],
                                publish_id=resp['snippet']['topLevelComment']['snippet']['publishedAt'],
                                comment_author=resp['snippet']['topLevelComment']['snippet']['authorDisplayName'])

                        comment_data.append(data)
  except:
      pass
  return comment_data




def channel_details(channel_id):
    ch_details=get_channels_details(channel_id)
    vi_ids=get_channel_videos_ids(channel_id)
    vi_details=get_video_information(vi_ids)
    com_details=get_comment_information(vi_ids)



    connection = db["channel_details_info"]
    connection .insert_one ({"channel_info":ch_details,"Video_info" :vi_ids,"video_info":vi_details,"comment_info":com_details })


    return "uploaded"






config = {
    'host': 'localhost',
    'user': 'root',
    'password': '12345',
    'charset': 'utf8mb4',
    'cursorclass': DictCursor,
}

connection = pymysql.connect(**config)
mycursor = connection.cursor()




mycursor.execute =('''create tables if not exist videos_information,(channel_name varchar(100),
                        Channel_id varchar(100)primary key,
                        video_ids varchar(100),
                        video_name varchar(100),
                        Tags varchar(100),
                        Thumbnails varchar(100),
                        Description text,
                        PublishedAt timestamp,
                        view_counts int ,
                        like_counts int,
                        favorite_count int,
                        comment_count int,
                        duration interval,
                        caption_status varchar(100))''')



sql='''insert into video_information(channel_name,
                                        Channel_id ,
                                        video_ids,
                                        video_name,
                                        Tags ,
                                        Thumbnails, 
                                        Description,
                                        PublishedAt,
                                        view_counts,
                                        like_counts,
                                        favorite_count,
                                        comment_count,
                                        duration,
                                        caption_status
                                        )
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''



for video in channel:
    values= (
                video['channel_name'],
                video['Channel_id'],
                video['video_ids'],
                video['video_name'],
                video['Tags'] ,
                video['Thumbnails'], 
                video['Description'],
                video['PublishedAt'],
                video['view_counts'],
                video['like_counts'],
                video['favorite_count'],
                video[' comment_count'],
                video['duration'],
                video[' caption_status']
                               )

mycursor.execute(pymysql,values)

config.commit()




