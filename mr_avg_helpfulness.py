"""
Task 1.3.5 - Calculate the average helpfulness score for reviews.

Helpfulness score = helpful_votes / total_votes
(only computed for reviews that have at least one vote).

MapReduce job that maps each review to (asin, helpfulness_score)
and reduces by computing the average helpfulness score per product.
Works with JSONL files where each line is a review object.

Note: industrial_reviews.json does not contain helpfulness data.
This code is designed for datasets that have 'helpful' field with [helpful_votes, total_votes].
"""

from mrjob.job import MRJob
import json


class MRAvgHelpfulness(MRJob):

    def mapper(self, _, line):
        """
        Map phase: Process each review and emit product ID with helpfulness score.
        
        Args:
            _: Line key (unused)
            line: A single line from the JSONL file containing a review JSON object
        
        Yields:
            (asin, helpfulness_score): Product ID and helpfulness ratio (0.0 to 1.0)
        """
        try:
            # Parse the JSON line into a Python dictionary
            data = json.loads(line)
            # Extract the product ID (asin)
            asin = data.get('asin', '').strip()
            # Get helpfulness data: expected format is [helpful_votes, total_votes]
            helpful = data.get('helpful')
            
            if asin and helpful:
                # Parse the helpfulness list
                if isinstance(helpful, list) and len(helpful) == 2:
                    helpful_votes = int(helpful[0])
                    total_votes = int(helpful[1])
                    # Only include reviews that received at least one vote
                    if total_votes > 0:
                        score = helpful_votes / total_votes
                        yield asin, score
        except Exception:
            # Skip malformed JSON lines or invalid helpfulness data
            pass

    def reducer(self, asin, scores):
        """
        Reduce phase: Calculate the average helpfulness score for each product.
        
        Args:
            asin: Product ID
            scores: Iterator of helpfulness scores from all reviews for this product
        
        Yields:
            (asin, avg_helpfulness): Product ID and average helpfulness score (0-1)
        """
        total = 0.0
        count = 0
        # Sum all helpfulness scores and count them
        for s in scores:
            total += s
            count += 1
        if count > 0:
            # Calculate and return average helpfulness score (rounded to 4 decimals)
            yield asin, round(total / count, 4)


if __name__ == '__main__':
    MRAvgHelpfulness.run()
