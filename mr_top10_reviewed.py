"""
Task 1.3.4 - Find the top ten most reviewed products.

Two-step MapReduce job:
  Step 1 - Count reviews per product (same as Task 1.3.2).
  Step 2 - Collect all counts into a single reducer and emit the top 10.
Works with JSONL files where each line is a review object.
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import json


class MRTop10Reviewed(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count,
                   reducer=self.reducer_count),
            MRStep(mapper=self.mapper_swap,
                   reducer=self.reducer_top10),
        ]

    # ── Step 1: count reviews per product ──────────────────────────────────

    def mapper_count(self, _, line):
        """
        Step 1 Mapper: Process each review and emit product ID with count of 1.
        
        Args:
            _: Line key (unused)
            line: A single line from the JSONL file containing a review JSON object
        
        Yields:
            (asin, 1): Product ID and count of 1 for each valid review
        """
        try:
            # Parse the JSON line into a Python dictionary
            data = json.loads(line)
            # Extract the product ID (asin)
            asin = data.get('asin', '').strip()
            if asin:
                # Emit the product ID with a count of 1
                yield asin, 1
        except Exception:
            # Skip malformed JSON lines
            pass

    def reducer_count(self, asin, counts):
        """
        Step 1 Reducer: Sum all review counts for each product.
        
        Args:
            asin: Product ID
            counts: Iterator of count values (all 1s from mapper)
        
        Yields:
            (asin, total_count): Product ID and total number of reviews
        """
        # Sum all counts for this product
        yield asin, sum(counts)

    # ── Step 2: sort and select top 10 ─────────────────────────────────────

    def mapper_swap(self, asin, count):
        """
        Step 2 Mapper: Swap key-value to prepare for sorting by count.
        Emits constant key (None) so all products go to a single reducer.
        
        Args:
            asin: Product ID
            count: Total review count for this product
        
        Yields:
            (None, (count, asin)): Constant key with count-product pair
        """
        # Emit constant key so all pairs go to one reducer for global sorting
        yield None, (count, asin)

    def reducer_top10(self, _, pairs):
        """
        Step 2 Reducer: Sort all products by review count and emit top 10.
        
        Args:
            _: Constant key (None)
            pairs: Iterator of (count, asin) tuples from all products
        
        Yields:
            (rank, product_info): Rank (1-10) and dict with asin and review count
        """
        # Sort all products by count in descending order and take top 10
        top = sorted(pairs, key=lambda x: x[0], reverse=True)[:10]
        # Emit each top product with its rank
        for rank, (count, asin) in enumerate(top, start=1):
            yield rank, {'asin': asin, 'review_count': count}


if __name__ == '__main__':
    MRTop10Reviewed.run()
