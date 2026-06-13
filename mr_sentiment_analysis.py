"""
Task - Perform sentiment analysis on review texts to categorize them as positive, neutral, or negative.

Sentiment classification based on review text content using keyword matching:
- Positive: Contains positive keywords (excellent, great, love, amazing, perfect, etc.)
- Negative: Contains negative keywords (terrible, awful, hate, worst, poor, etc.)
- Neutral: Mixed or no strong sentiment keywords

MapReduce job that maps each review to its sentiment category
and reduces by counting reviews in each sentiment category per product.
Works with JSONL files where each line is a review object.
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re


class MRSentimentAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_sentiment,
                   reducer=self.reducer_count_sentiment),
            MRStep(reducer=self.reducer_aggregate_sentiment)
        ]

    def mapper_sentiment(self, _, line):
        """
        Map phase: Process each review text and emit product ID with sentiment category.
        
        Args:
            _: Line key (unused)
            line: A single line from the JSONL file containing a review JSON object
        
        Yields:
            (asin, sentiment): Product ID and sentiment category (positive/neutral/negative)
        """
        # Define sentiment keywords (optimized for speed)
        positive_words = ['excellent', 'great', 'love', 'amazing', 'perfect', 
                         'wonderful', 'fantastic', 'awesome', 'best', 'good', 
                         'delicious', 'recommend']
        
        negative_words = ['terrible', 'awful', 'hate', 'worst', 'poor', 'bad', 
                         'horrible', 'disgusting', 'disappointing', 'waste', 
                         'disappointed', 'nasty']
        
        try:
            # Parse the JSON line into a Python dictionary
            data = json.loads(line)
            # Extract the product ID (asin) and review text
            asin = data.get('asin', '').strip()
            review_text = data.get('reviewText', '')
            summary = data.get('summary', '')
            
            if asin and (review_text or summary):
                # Combine review text and summary for analysis (lowercase)
                full_text = (review_text + ' ' + summary).lower()
                
                # Count positive and negative keywords (simple substring match)
                positive_count = sum(1 for word in positive_words if word in full_text)
                negative_count = sum(1 for word in negative_words if word in full_text)
                
                # Classify sentiment based on keyword counts
                if positive_count > negative_count:
                    sentiment = 'positive'
                elif negative_count > positive_count:
                    sentiment = 'negative'
                else:
                    # If equal or both zero, use neutral
                    sentiment = 'neutral'
                
                # Emit product ID with sentiment category
                yield asin, sentiment
        except Exception:
            # Skip malformed JSON lines
            pass

    def reducer_count_sentiment(self, asin, sentiments):
        """
        Reduce phase: Count reviews in each sentiment category for each product.
        
        Args:
            asin: Product ID
            sentiments: Iterator of sentiment categories from all reviews for this product
        
        Yields:
            (asin, sentiment_counts): Product ID and dict with counts per sentiment
        """
        # Initialize counters for each sentiment
        sentiment_counts = {
            'positive': 0,
            'neutral': 0,
            'negative': 0
        }
        
        # Count occurrences of each sentiment
        for sentiment in sentiments:
            sentiment_counts[sentiment] += 1
        
        # Calculate total reviews and percentages
        total = sum(sentiment_counts.values())
        
        if total > 0:
            result = {
                'total_reviews': total,
                'positive': sentiment_counts['positive'],
                'neutral': sentiment_counts['neutral'],
                'negative': sentiment_counts['negative'],
            }
            
            yield asin, result

    def reducer_aggregate_sentiment(self, asin, results):
        """
        Final reduce phase: Emit the sentiment analysis results for each product.
        
        Args:
            asin: Product ID
            results: Iterator of sentiment count dictionaries (should be only one)
        
        Yields:
            (asin, sentiment_data): Product ID and complete sentiment analysis
        """
        # Since there's only one result per product from previous reducer
        for result in results:
            yield asin, result


if __name__ == '__main__':
    MRSentimentAnalysis.run()
