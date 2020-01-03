from ksql import KSQLAPI
import exceptions
client = KSQLAPI('http://localhost:8088')



client.ksql("show stream;")

"""while True :
    query = client.query('select count(*),max(ROWTIME),min(ROWTIME),user_lang as tweet_count from tweetinfo WINDOW TUMBLING (SIZE 30 SECONDS) group by user_lang')
    for item in query:
        print(item)"""