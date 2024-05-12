import asyncio
import warnings

warnings.filterwarnings("ignore")
from dataclasses import dataclass
from typing import List, Tuple
from transformers import pipeline
from supabase_handler import Text, SupabaseHandler

# classifier = pipeline("text-classification",model='bhadresh-savani/distilbert-base-uncased-emotion', top_k=None)
# test = """
# Vineesh: You suck lol
# Shrvaya: I know right
# Shravya: I am so happy today
# Vineesh: I am so sad today
# Shravya: Why
# Vineesh: I wanted to meet you
# Shravya: We can meet tomorrow
# """
# prediction = classifier(test, )
# print(prediction)

# """
# Output:
# [[
# {'label': 'sadness', 'score': 0.0006792712374590337}, 
# {'label': 'joy', 'score': 0.9959300756454468}, 
# {'label': 'love', 'score': 0.0009452480007894337}, 
# {'label': 'anger', 'score': 0.0018055217806249857}, 
# {'label': 'fear', 'score': 0.00041110432357527316}, 
# {'label': 'surprise', 'score': 0.0002288572577526793}
# ]]
# """

@dataclass
class PredictedLabel:
    label : str
    score : float
    positve : bool = False
    
    def __post_init__(self):
        self.positve = self.label in ['joy', 'love', 'surprise']


class SentimentAnalyzer:
    def __init__(self) -> None:
        self.classifier = pipeline("text-classification",model='bhadresh-savani/distilbert-base-uncased-emotion', top_k=None)
        self.handler = SupabaseHandler()
        
    async def analyze_sentiment(self, chunk_size : int = 15) -> List[Tuple[str, float]]:
        found = False
        while not found:
            texts = await self.handler.get_random_text(chunk_size = chunk_size)
            formatted_texts = await self.format_texts(texts)
            prediction = self.classifier(formatted_texts)[0][0]
            pred_obj = PredictedLabel(prediction['label'], prediction['score'])
            if pred_obj.positve and pred_obj.score >= 0.95:
                found = True
                await self.handler.insert_today(texts[0][0], chunk_size = chunk_size)
                print(f"Found positive sentiment: {pred_obj.label} with a score of {pred_obj.score}")
                # print(formatted_texts)
                break
    
    @staticmethod   
    async def format_texts(texts : List[Tuple[str, Text]]) -> str:
        return "\n".join([f"{text[1].person}: {text[1].text_body}" for text in texts])
    

if __name__ == '__main__':
    analyzer = SentimentAnalyzer()
    asyncio.run(analyzer.analyze_sentiment())