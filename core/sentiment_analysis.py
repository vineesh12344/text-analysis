import asyncio
import warnings

warnings.filterwarnings("ignore")
from dataclasses import dataclass
from functools import reduce
import operator
from typing import List, Tuple, Any, Dict, Iterable, cast
from transformers import pipeline
from supabase_handler import Text, SupabaseHandler


@dataclass
class PredictedLabel:
    label: str
    score: float
    positve: bool = False

    def __post_init__(self):
        self.positve = self.label in ["joy", "love", "surprise"]


class SentimentAnalyzer:
    def __init__(self) -> None:
        self.classifier = pipeline(
            "text-classification",
            model="bhadresh-savani/distilbert-base-uncased-emotion",
            top_k=None,
        )
        self.handler = SupabaseHandler()

    async def analyze_sentiment(self, chunk_size: int = 15) -> None:
        found = False
        while not found:
            texts = await self.handler.get_random_text(chunk_size=chunk_size)
            sentiment_scores = cast(
                List[Any], self.classifier(await self.format_texts(texts))
            )
            pred_obj = await self.flatten_and_parse(sentiment_scores)
            if pred_obj.positve and pred_obj.score >= 0.95:
                found = True
                await self.handler.insert_today(texts[0][0], chunk_size=chunk_size)
                print(
                    f"Found positive sentiment: {pred_obj.label} with a score of {pred_obj.score}"
                )
                # print(formatted_texts)
                break

    @staticmethod
    async def format_texts(texts: List[Tuple[str, Text]]) -> str:
        return "\n".join([f"{text[1].person}: {text[1].text_body}" for text in texts])

    @staticmethod
    async def flatten_and_parse(
        scores: List[List[Dict[str, str | int]]]
    ) -> PredictedLabel:
        out = reduce(operator.concat, scores)
        predicted_labels = [
            PredictedLabel(cast(str, score["label"]), cast(float, score["score"]))
            for score in out
        ]
        predicted_labels.sort(key=lambda x: x.score, reverse=True)
        return predicted_labels[0]


if __name__ == "__main__":
    analyzer = SentimentAnalyzer()
    asyncio.run(analyzer.analyze_sentiment())
