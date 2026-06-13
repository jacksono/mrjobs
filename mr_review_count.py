"""
MapReduce job that counts the number of reviews per product (asin).
Works with JSONL files where each line is a review object.
"""

from mrjob.job import MRJob
import json


class MRReviewCount(MRJob):

    def mapper(self, _, line):
        """
        Map phase: Process each review line and emit product ID with count of 1.
        
        Args:
            _: Line key (unused)
            line: A single line from the JSONL file containing a review JSON object
        
        Yields:
            (asin, 1): Product ID and count of 1 for each valid review
        """
        try:
            # Parse the JSON line into a Python dictionary
            data = json.loads(line)
            # Extract the product ID (asin) from the review
            asin = data.get('asin', '').strip()
            if asin:
                # Emit the product ID with a count of 1
                yield asin, 1
        except Exception:
            # Skip malformed JSON lines
            pass

    def combiner(self, asin, counts):
        """
        Combiner phase: Locally aggregate counts for each product before shuffle.
        Reduces network traffic by pre-summing counts on the mapper machine.
        
        Args:
            asin: Product ID
            counts: Iterator of count values (all 1s from mapper)
        
        Yields:
            (asin, total): Product ID and local sum of counts
        """
        # Sum all counts for this product on this machine
        yield asin, sum(counts)

    def reducer(self, asin, counts):
        """
        Reduce phase: Aggregate all counts for each product across all mappers.
        Receives grouped data after shuffle/sort and produces final counts.
        
        Args:
            asin: Product ID
            counts: Iterator of count values from all mappers/combiners
        
        Yields:
            (asin, total): Product ID and total number of reviews
        """
        # Sum all counts from all machines to get final review count
        yield asin, sum(counts)

if __name__ == '__main__':
    MRReviewCount.run()


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
        """
        # Define sentiment keywords (optimized for speed)
        positive_words = ['excellent', 'great', 'love', 'amazing', 'perfect', 
                         'wonderful', 'fantastic', 'awesome', 'best', 'good', 
                         'delicious', 'recommend']
        
        negative_words = ['terrible', 'awful', 'hate', 'worst', 'poor', 'bad', 
                         'horrible', 'disgusting', 'disappointing', 'waste', 
                         'disappointed', 'nasty']
        
        try:
            data = json.loads(line)
            asin = data.get('asin', '').strip()
            review_text = data.get('reviewText', '')
            summary = data.get('summary', '')
            
            if asin and (review_text or summary):
                full_text = (review_text + ' ' + summary).lower()
                
                # Count positive and negative keywords
                positive_count = sum(1 for word in positive_words if word in full_text)
                negative_count = sum(1 for word in negative_words if word in full_text)
                
                # Classify sentiment
                if positive_count > negative_count:
                    sentiment = 'positive'
                elif negative_count > positive_count:
                    sentiment = 'negative'
                else:
                    sentiment = 'neutral'
                
                yield asin, sentiment
        except Exception:
            pass

    def reducer_count_sentiment(self, asin, sentiments):
        """
        Reduce phase: Count reviews in each sentiment category for each product.
        """
        sentiment_counts = {'positive': 0, 'neutral': 0, 'negative': 0}
        
        for sentiment in sentiments:
            sentiment_counts[sentiment] += 1
        
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
        """Final reduce phase: Emit the sentiment analysis results."""
        for result in results:
            yield asin, result
