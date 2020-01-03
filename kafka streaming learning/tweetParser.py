import json

def tweetParse(msg):
    all_data = json.loads(msg)
    created_at = all_data['created_at']
    tweet_id = all_data['id']
    tweet_text = all_data['text'].replace (",",'').replace("\n","")
    source = all_data['source'].replace (",",'')

    user_info = all_data['user']
    user_name = user_info['name'].replace (",",'')
    user_screen_name = user_info['screen_name'].replace (",",'')
    if user_info['lang'] is not None :
        user_lang = user_info['lang'].replace (",",'')
    else :user_lang = ''

    return created_at,tweet_id,tweet_text,source,user_name,user_screen_name,user_lang

def extract_hash_tags(s):
    return list(part[1:] for part in s.split() if part.startswith('#'))


