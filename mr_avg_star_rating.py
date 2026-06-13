"""
Task 1.3.3 - Determine the average star rating for each product.

MapReduce job that maps each review to (asin, overall_rating)
and reduces by computing the average star rating per product.
Works with JSONL files where each line is a review object.
"""

from mrjob.job import MRJob
import json


class MRAvgStarRating(MRJob):

    def mapper(self, _, line):
        """
        Map phase: Process each review line and emit product ID with its rating.
        
        Args:
            _: Line key (unused)
            line: A single line from the JSONL file containing a review JSON object
        
        Yields:
            (asin, rating): Product ID and its star rating (1.0 to 5.0)
        """
        try:
            # Parse the JSON line into a Python dictionary
            data = json.loads(line)
            # Extract the product ID (asin) and rating (overall)
            asin = data.get('asin', '').strip()
            overall = data.get('overall')
            if asin and overall is not None:
                # Emit the product ID with its rating
                yield asin, float(overall)
        except Exception:
            # Skip malformed JSON lines or invalid ratings
            pass

    def reducer(self, asin, ratings):
        """
        Reduce phase: Calculate the average star rating for each product.
        
        Args:
            asin: Product ID
            ratings: Iterator of rating values from all reviews for this product
        
        Yields:
            (asin, avg_rating): Product ID and average rating rounded to 2 decimals
        """
        total = 0.0
        count = 0
        # Sum all ratings and count them
        for r in ratings:
            total += r
            count += 1
        if count > 0:
            # Calculate and return average rating
            yield asin, round(total / count, 2)


if __name__ == '__main__':
    MRAvgStarRating.run()
