# Kafka-Python-telegram_bot

To learn and understand more about kafka, this particular code is written.
Brief overview about the use case of this -
1. integrate the google/youtube API in python to fetch the playlist details like video_id, title and many more.
2. Once the details are fetched send these details to the cloud based kafka server(in this case confluent-kafka server is used)
3. For any change in the views count the table in confluent-kafka server will get updated.
4. connector is setup in confluent kafka which will publish the updated values to the telegram bot.

Please refer below youtube video link for better understanding of the confluent-kafka related services.
https://www.youtube.com/watch?v=jItIQ-UvFI4

Please note that the code was written with the intention of learning, I have also reffered the above video.
As and when i will update the code by adding additional functioanlities I'll update it in this repo as well.
