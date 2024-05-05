import json
import os
from dotenv import load_dotenv
import jsonlines
import uuid

load_dotenv()

with open(os.getenv('JSON_TEXTS')) as f:
    data = json.load(f)

with jsonlines.open(os.getenv('JSONL_TEXTS'), 'w') as writer:
    messages = data['messages']
    seen = set()
    for i, message in enumerate(messages):
        uniqueid = str(uuid.uuid1())
        if uniqueid in seen:
            print("Duplicate")
            break
        
        seen.add(uniqueid)
        if message["type"] == "message" and message["text"] != "":
            writer.write({"date" : message['date'], "text_body" : message['text'], "person" : message['from'], 'id' : uniqueid, "order" : i})

