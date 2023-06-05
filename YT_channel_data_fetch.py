from googleapiclient.discovery import build
import pandas as pd
import os
import datetime
import csv

api_key = 'AIzaSyBSfbW8KfSmjqQaEErtu3131QqNM4PeUR4'

files = [
    'business_channel_ids.csv',
    'dating_channel_ids.csv',
    'extra_channel_ids.csv',
    'ielts_channel_ids.csv',
    'self_channel_ids.csv',
    'study_channel_ids.csv'
    ]

youtube = build('youtube', 'v3', developerKey=api_key)

# Function to necessary folders for csv storage
def folder_creations(folder):
    if os.path.isdir(f'files') == False:
        print('creating main directory for output files')
        os.mkdir(f'files')
    if os.path.isdir(f'files/{folder}') == False:
        print('creating sub directory for output files')
        os.mkdir(f'files/{folder}')

# Function to get channel statistics
def get_channel_stats(request_counter,youtube, channel_ids):
    all_data = []
    request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=','.join(channel_ids))
    response = request.execute()
    request_counter += 1 
    for i in range(len(response['items'])):
        data = dict(Channel_name = response['items'][i]['snippet']['title'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        all_data.append(data)
    return all_data,request_counter

# Function to get videos associated channel
def get_video_ids(request_counter,youtube, playlist_id,upto_date):
    request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50)
    response = request.execute()
    request_counter += 1
    video_ids = []
    for i in range(len(response['items'])):
        if upto_date != '':
            video_date = response['items'][i]['contentDetails']['videoPublishedAt'].split('T')[0]
            if datetime.datetime.strptime(upto_date,'%Y-%m-%d') < datetime.datetime.strptime(video_date,'%Y-%m-%d'):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            else:
                return video_ids,request_counter
        else:
            video_ids.append(response['items'][i]['contentDetails']['videoId'])
    next_page_token = response.get('nextPageToken')
    more_pages = True
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()
            request_counter += 1
            for i in range(len(response['items'])):
                if upto_date != '':
                    video_date = response['items'][i]['contentDetails']['videoPublishedAt'].split('T')[0]
                    if datetime.datetime.strptime(upto_date,'%Y-%m-%d') < datetime.datetime.strptime(video_date,'%Y-%m-%d'):
                        video_ids.append(response['items'][i]['contentDetails']['videoId'])
                    else:
                        return video_ids,request_counter
                else:
                    video_ids.append(response['items'][i]['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')
    return video_ids,request_counter

# Function to get video details
def get_video_details(request_counter,youtube, video_ids):
    all_video_stats = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
                    part='snippet,statistics',
                    id=','.join(video_ids[i:i+50]))
        response = request.execute()
        request_counter += 1
        for video in response['items']:
            title = video['snippet']['title'],
            url = 'https://www.youtube.com/watch?v='+video['id']
            p_date = video['snippet']['publishedAt']
            try:
                views= video['statistics']['viewCount']
            except KeyError:
                views = 0
            video_stats = dict(Title = title[0].replace('"',''),video_url = url,Published_date = p_date,Views = views,)
            all_video_stats.append(video_stats)
    return all_video_stats,request_counter

# Function to process channel data
def process_channel(channel_id,upto_date):
    request_counter = 0
    channel_statistics,request_counter = get_channel_stats(request_counter,youtube, [channel_id])
    channel_data = pd.DataFrame(channel_statistics)
    channel_data['Subscribers'] = pd.to_numeric(channel_data['Subscribers'])
    channel_data['Views'] = pd.to_numeric(channel_data['Views'])
    channel_data['Total_videos'] = pd.to_numeric(channel_data['Total_videos'])
    channel_data.dtypes
    channel_name = channel_data['Channel_name'][0]
    
    playlist_id = channel_data.loc[channel_data['Channel_name']==channel_name, 'playlist_id'].iloc[0]
    video_ids, request_counter = get_video_ids(request_counter,youtube, playlist_id,upto_date)
    if video_ids == []:
        return pd.DataFrame(),channel_name,request_counter
    video_details,request_counter = get_video_details(request_counter,youtube, video_ids)
    video_data = pd.DataFrame(video_details)
    video_data['Published_date'] = pd.to_datetime(video_data['Published_date']).dt.date
    video_data['Views'] = pd.to_numeric(video_data['Views'])
    # video_data['Likes'] = pd.to_numeric(video_data['Likes'])
    # video_data['Dislikes'] = pd.to_numeric(video_data['Dislikes'])
    video_data = video_data.sort_values(by=['Published_date'],ascending=False)
    return video_data,channel_name,request_counter

# Function to process channels file
def process_file(file):
    print(f'scraping data for file: {file}')
    folder = file.split('_')[0]
    folder_creations(folder)
    channels_df = pd.read_csv(str('channels/'+file))
    total_req = 0
    for ch_row in channels_df.iterrows():
        print('-'*100)
        channel_id = ch_row[1]['Channel Id']
        channel_name = ch_row[1]['Channel Name']
        existing_out_file = f'files/{folder}/{channel_name}_videos_data.csv'
        csv_file = existing_out_file.split('/')[-1]
        upto_date = ''
        out_file = pd.DataFrame()
        if os.path.isfile(existing_out_file) == True:
            print(f'Loading Existing File {csv_file}')
            out_file = pd.read_csv(existing_out_file,index_col=None)
            upto_date = out_file.head(1)['Published_date'].iloc[0]
        else:
            print(f'Existing File Not Found {csv_file}, Creating New File')
        video_data,channel_name,request_counter = process_channel(channel_id,upto_date)
        csv_file = channel_name.split(':')[0].replace('|','').strip()+'_videos_data.csv'
        if video_data.empty:
            print('channel data is upto date')
        if out_file.empty:
            video_data.to_csv(f'files/{folder}/'+csv_file,index=False,quotechar='"',quoting=csv.QUOTE_NONNUMERIC,escapechar="\\")
        if not out_file.empty and not video_data.empty:
            out_file = pd.concat([out_file,video_data],ignore_index=False)
            out_file['Published_date'] = pd.to_datetime(out_file['Published_date']).dt.date
            out_file = out_file.sort_values(by=['Published_date'],ascending=False)
            out_file.to_csv(f'files/{folder}/'+csv_file,index=False,quotechar='"',quoting=csv.QUOTE_NONNUMERIC,escapechar="\\")
        print(f"Request made during scraping the channel '{channel_name}': {request_counter}")
        total_req = total_req+request_counter
    print(f"Total request made during this run: {total_req}")

if __name__ == "__main__":
    ch_num = int(input("1. Business channels \n2. Dating channels \n3. Extra channels \n4. Ielts channel \n5. Self channels \n6. Study channels \nEnter Your Choice: "))
    # ch_num = 2
    file = files[ch_num-1]
    process_file(file)