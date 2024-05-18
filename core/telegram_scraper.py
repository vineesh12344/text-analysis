import sys

sys.path.append("./")
import os
import asyncio
import json
from typing import List, Dict, Deque,AsyncGenerator, cast
from collections import deque
import jsonlines
from dotenv import load_dotenv
from dateutil import parser
from pyrogram.client import Client
from pyrogram.types import Message
from supabase_handler import SupabaseHandler, Text

load_dotenv()

DATA_DIR_PATH = './'

class TelegramScraper:
    def __init__(self) -> None:
        """
        Constructs a TelegramScraper instance for a specific Telegram channel.

        This constructor prepares the Telegram Client by loading API credentials from the environment variables.
        It also creates a directory for session data and initializes a list to hold the scraped messages from the designated Telegram channel.

        Parameters:
            channel (str): The identifier of the Telegram channel from which messages will be scraped.
        """
        os.makedirs("temp_data/telegram_sessions", exist_ok=True)
        self.CONFIG = {
            "telegram_api_id": int(cast(str, os.getenv("TG_API_ID"))),
            "telegram_hash": os.getenv("TG_API_HASH"),
            "phone_number": os.getenv("TG_PHONE_NUMBER"),
        }
        contact =os.getenv("GF_NUMBER")
        
        assert contact is not None, "Please provide the channel id"
        
        self.contact = contact
        self.messages: Deque[Text] = deque()
        self.supabase = SupabaseHandler()

    async def get_channel_messages(self, app: Client) -> None:
        """
        Asynchronously retrieves messages from the specified Telegram channel and stores them.

        This method iterates over the message history of the channel, filtering out messages without text,
        and appends each message's ID and text to the `messages` list attribute of the TelegramScraper object.
        """
        last_checks = (None, None)
        with jsonlines.open("result.jsonl") as reader:
            last_line = None
            for line in reader:
                last_line = line
                
            if last_line is not None:
                last_checks = (parser.parse(last_line["date"]).isoformat(), last_line["text_body"])
        
        async with app:
            message: Message          
            async for message in cast( AsyncGenerator, app.get_chat_history(chat_id=self.contact)):
                if message.text is not None:
                    text = message.text
                    
                    if message.date.isoformat() == last_checks[0] and text == last_checks[1]:
                        break

                    if text != "":
                        from_user = message.from_user.username if message.from_user.username is not None else "Vineesh Nimmagadda"
                        self.messages.appendleft(Text(date = message.date.isoformat(), text_body = text, person = from_user, order = 0))

    async def main(self) -> None:
        """
        Executes the message retrieval process asynchronously.

        Initiates the asynchronous retrieval of messages from a specified Telegram channel by invoking the `get_channel_messages` method. This process runs to completion before exiting.
        """
        app = Client(
            name="telegram_scraper",
            api_id=self.CONFIG["telegram_api_id"],
            api_hash=self.CONFIG["telegram_hash"],
            phone_number=self.CONFIG["phone_number"],
            workdir="temp_data/telegram_sessions",
        )

        await self.get_channel_messages(app)
        
        curr = await self.supabase.get_count(table = "all_texts")
        
        for i, message in enumerate(self.messages, 1):
            message.order = curr + i
            
        await self.supabase.insert_texts(self.messages, table = "all_texts", chunk_size = 10000)      


if __name__ == "__main__":
    scraper = TelegramScraper()
    asyncio.run(scraper.main())
