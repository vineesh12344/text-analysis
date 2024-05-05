import os
from dataclasses import dataclass
import asyncio
import datetime
from typing import List
import jsonlines
from dotenv import load_dotenv
from dateutil import parser
from supabase_py_async import create_client, AsyncClient

load_dotenv()

@dataclass
class Text:
    date : datetime.datetime
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
        self.client : AsyncClient= asyncio.run(create_client(url, key))
        self.seen = set()

    async def insert_texts(self, text : Text | List[Text], table : str = "all_texts", chunk_size : int = 10000) -> None:
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
        response = await self.client.table(table).select("*", count = 'exact').execute()
        return response.count
                
    async def insert_texts_bulk(self, filepath : str, table : str = "all_texts") -> None:
        texts = []
        with jsonlines.open(filepath, 'r') as reader:
            for i,line in enumerate(reader, 1):
                texts.append(Text(line['date'], line['text_body'], line['person'], order = i))
                
        await self.insert_texts(texts, table = table)

if __name__ == '__main__':
    handler = SupabaseHandler()
    asyncio.run(handler.insert_texts_bulk(os.getenv('JSONL_TEXTS')))
    # print(uuid.uuid1().is_safe)
