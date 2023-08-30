#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install confluent-kafka


# In[1]:


pip install confluent-kafka configparser


# In[1]:


def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


# In[3]:


get_ipython().run_line_magic('ls', '')


# In[22]:


response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", 
                        params = {"key":"google api key", 
                                  "playlistId":"playlist id",
                                  "part":"contentDetails"})
print(response.text)


# In[37]:


import json
#FUNCTION for fetching the details from specific page in the playlist
def fetch_items_page(page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", 
                        params = {"key":"google api key", 
                                  "playlistId":"playlist id",
                                  "part":"contentDetails", 
                                  "pageToken":page_token})
    return json.loads(response.text)


# In[33]:


if __name__ == '__main__':
    print(fetch_items_page())


# In[39]:


#funtion to iterate over the playlists' multiple pages
def fetch_playlist_item(page_token = None):
    payload = fetch_items_page(page_token)
    yield from payload['items']
    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None:
        yield from fetch_playlist_item(next_page_token)


# In[40]:


for items in fetch_playlist_item():
    print(items)


# In[60]:


for page_items in fetch_playlist_item():
    video_id = page_items["contentDetails"]["videoId"]
    for video in fetch_videos(video_id):
        print(video)


# In[157]:


def get_video_page(video_id):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos", 
                            params={"key":"google api key", 
                                    "id":video_id, 
                                    "part":"snippet, statistics"})
    return json.loads(response.text)


# In[69]:


get_video_page("video_id")


# In[74]:


def get_video(video_id):
    payload = get_video_page(video_id)
    yield from payload['items']


# In[84]:


for playlist_items in fetch_playlist_item():
    video_id = playlist_items['contentDetails']['videoId']
    for video in get_video(video_id):
        print(pformat(video))


# In[86]:


def summerize_video(video):
    return {
        "video_id":video['id'],
        "title":video['snippet']['title'],
        "views":video['statistics']['viewCount']
    }


# In[87]:


for playlist_items in fetch_playlist_item():
    video_id = playlist_items['contentDetails']['videoId']
    for video in get_video(video_id):
        print(pformat(summerize_video(video)))


# In[155]:


from confluent_kafka import SerializingProducer


schema_registry_client = SchemaRegistryClient(config['schema_registry'])
youtube_video_schema = schema_registry_client.get_latest_version('youtube_video-value')

kafka_config = config['kafka'] | {
    "key.serializer":StringSerializer(),
    "value.serializer":AvroSerializer(schema_registry_client, youtube_video_schema.schema.schema_str)
}
producer = SerializingProducer(kafka_config)

for playlist_items in fetch_playlist_item():
    video_id = playlist_items['contentDetails']['videoId']
    for video in get_video(video_id):
        producer.produce(
            topic="youtube_video",
            key = video_id, 
            value = {"TITLE":video['snippet']['title'], 
                     "VIEWS":int(video['statistics']['viewCount']),
                    },
            on_delivery = ondelivery
        )
producer.flush()


# In[145]:


def ondelivery(err, record):
    pass


# In[156]:


config = {
    "google_api_key":"google api key",
    "kafka":
        {
            "bootstrap.servers":"bootstrap_api", 
            "security.protocol":"sasl_ssl", 
            "sasl.mechanism":"PLAIN", 
            "sasl.username":"key", 
            "sasl.password":"secret"
        },
    "schema_registry":{"url":"API" ,
                       "basic.auth.user.info":"key:secret"
                      }
}


# In[134]:


from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# In[158]:


#for this project I have used confluent kafka library.


# In[ ]:




