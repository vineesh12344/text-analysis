import os
from dataclasses import dataclass, field
import asyncio
import random
import uuid
from datetime import datetime, timezone
from typing import List, Tuple, cast, Iterable
import pytz
import jsonlines
from dotenv import load_dotenv
from dateutil import parser
from supabase_py_async import create_client, AsyncClient
from postgrest.types import CountMethod

load_dotenv()

@dataclass
class Text:
    date : str
    text_body : str
    person : str
    order : int
    
    def __post_init__(self):
        date = parser.parse(self.date)
        self.date = date.isoformat()

class SupabaseHandler:
    def __init__(self) -> None:
        url = os.getenv('PROJECT_URL')
        key = os.getenv('API_KEY')
        
        assert url is not None, "Please provide the project url"
        assert key is not None, "Please provide the api key"
        
        self.client : AsyncClient= asyncio.run(create_client(url, key))
        self.seen = set()

    async def insert_texts(self, text : Text | Iterable[Text], table : str = "all_texts", chunk_size : int = 10000) -> None:
        inserts : List[dict] | dict = vars(text) if isinstance(text, Text) else [vars(t) for t in text]
        try:
            if len(inserts) == 1:
                await self.client.table(table).upsert(inserts).execute()
            else:
                num_chunks = len(inserts) // chunk_size + 1
                for chunk_num, i in enumerate(range(0, len(inserts), chunk_size)):
                    await self.client.table(table).upsert(inserts[i:i+chunk_size]).execute()
                    print(f"Progress {chunk_num + 1}/{num_chunks}")        
        except Exception as e:
            print(e)
                
    async def get_count(self, table : str = "all_texts") -> int:
        response = await self.client.table(table).select("*", count = cast(CountMethod, 'exact')).execute()
        if response.count is None:
            return 0
        
        return response.count
                
    async def insert_texts_bulk(self, filepath : str, table : str = "all_texts") -> None:
        texts = []
        with jsonlines.open(filepath, 'r') as reader:
            for i,line in enumerate(reader, 1):
                texts.append(Text(line['date'], line['text_body'], line['person'], order = i))
                
        await self.insert_texts(texts, table = table)
        
    async def get_random_text(self, table : str = "all_texts", chunk_size = 15) -> List[Tuple[str, Text]]:
        rand_index = random.randint(0, await self.get_count(table))
        response = await self.client.table(table).select("*").gte('"order"', rand_index).order('"order"').limit(chunk_size).execute()
        all_data = response.data 
        return [(data.pop('id'), Text(**data)) for data in all_data]
    
    async def insert_today(self, id : str, chunk_size : int) -> None:
        try:
            timestamp = datetime.now(tz=pytz.timezone('Asia/Hong_Kong')).isoformat()
            insert_obj = {"text_id" : id, "take" : chunk_size, "date" : timestamp}
            response = await self.client.table("today_chunk").insert(insert_obj).execute()
        except Exception as e:
            print(e)
            
    async def get_last_message(self, table : str = "all_texts") -> Text:
        response = await self.client.table(table).select("*").order('"order"', desc = True).limit(1).execute()
        text = response.data[0]
        text.pop('id')
        return Text(**text)

if __name__ == '__main__':
    handler = SupabaseHandler()
    # asyncio.run(handler.insert_texts_bulk(os.getenv('JSONL_TEXTS')))
    # print(asyncio.run(handler.get_random_text()))
    # print(uuid.uuid1().is_safe)
    print(asyncio.run(handler.get_last_message()))
