import sys

sys.path.append("./")
import asyncio
import nest_asyncio
nest_asyncio.apply()
from core.sentiment_analysis import SentimentAnalyzer
from core.telegram_scraper import TelegramScraper

async def main() -> None:
    """
    Executes the daily job asynchronously.

    Initiates the asynchronous execution of the daily job, which involves the retrieval of messages from a specified Telegram channel, the analysis of the sentiment of a random message, and the insertion of the message into the Supabase database.
    """
    scraper = TelegramScraper()
    await scraper.main()

    analyzer = SentimentAnalyzer()
    await analyzer.analyze_sentiment()
    
if __name__ == "__main__":
    asyncio.run(main())