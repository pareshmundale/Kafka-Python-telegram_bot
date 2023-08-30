pip install confluent-kafka
pip install confluent-kafka configparser

### to understand the structure of the response we receive from youtube/google API
response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", 
                        params = {"key":"google api key", 
                                  "playlistId":"playlist id",
                                  "part":"contentDetails"})
print(response.text)

###for modulerization adding functions
import json
#FUNCTION for fetching the details from specific page in the playlist
def fetch_items_page(page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", 
                        params = {"key":"google api key", 
                                  "playlistId":"playlist id",
                                  "part":"contentDetails", 
                                  "pageToken":page_token})
    return json.loads(response.text)

'''
if __name__ == '__main__':
    print(fetch_items_page())
'''

#funtion to iterate over the playlists' multiple pages
def fetch_playlist_item(page_token = None):
    payload = fetch_items_page(page_token)
    yield from payload['items']
    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None:
        yield from fetch_playlist_item(next_page_token)

###checking the output of the functions
for items in fetch_playlist_item():
    print(items)


for page_items in fetch_playlist_item():
    video_id = page_items["contentDetails"]["videoId"]
    for video in fetch_videos(video_id):
        print(video)

def get_video_page(video_id):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos", 
                            params={"key":"google api key", 
                                    "id":video_id, 
                                    "part":"snippet, statistics"})
    return json.loads(response.text)

get_video_page("video_id")


def get_video(video_id):
    payload = get_video_page(video_id)
    yield from payload['items']

for playlist_items in fetch_playlist_item():
    video_id = playlist_items['contentDetails']['videoId']
    for video in get_video(video_id):
        print(pformat(video))

def summerize_video(video):
    return {
        "video_id":video['id'],
        "title":video['snippet']['title'],
        "views":video['statistics']['viewCount']
    }


for playlist_items in fetch_playlist_item():
    video_id = playlist_items['contentDetails']['videoId']
    for video in get_video(video_id):
        print(pformat(summerize_video(video)))

###adding some additional libraries for further processing
from confluent_kafka import SerializingProducer

### this part of code can be added in the main function for simplicity
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

#creating the pass through function to send it as a value for producer object 
def ondelivery(err, record):
    pass


###necessary details needs to be maintained in below dictionary.
### google api key can be obtained from google developer account
###kafka related information can be received through any of the kafka service provide in this case I have used confluent kafka
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

###adding some more missing libraries.
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

'''
Please note that I have used jupyter notebook for the actual implementation of this project hence the sequence of the commands/code blogs might not be correct.
please adjust accordingly to the editor thaat you are using'''
