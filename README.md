<!-- @format -->

# MapReduce Review Analysis Report

## Summary

This repo presents a comprehensive analysis of Amazon review data using MapReduce framework with Python's MRJob library. The analysis was performed on the `grocery_reviews.json` dataset containing approximately 1.3 million reviews across 166,049 unique products.

---

## Dataset Overview

**Dataset:** `grocery_reviews.json`

- **Total Reviews:** 1,297,156
- **Unique Products:** 166,049
- **Format:** JSONL (JSON Lines) - one JSON object per line
- **Key Fields:** `asin`, `overall`, `reviewText`, `summary`, `helpful`, `reviewerID`, `reviewTime`

---

## Task 1: Review Count per Product

### Objective

Count the total number of reviews for each product (identified by ASIN).

### Methodology

- **Mapper:** Reads each review line, parses JSON, extracts `asin`, and emits `(asin, 1)`
- **Combiner:** Locally aggregates counts on mapper machines to reduce network traffic
- **Reducer:** Sums all counts for each product to produce final review counts

### Analysis

- The combiner optimization reduces network traffic by pre-aggregating counts locally
- Most products have relatively few reviews, but top products have thousands
- Efficient for large-scale counting operations

---

## Task 2: Average Star Rating per Product

### Objective

Calculate the average star rating (1.0 to 5.0) for each product.

### Methodology

- **Mapper:** Extracts `asin` and `overall` rating, emits `(asin, rating)`
- **Reducer:** Calculates average by summing all ratings and dividing by count

### Analysis

- Ratings are rounded to 2 decimal places for readability
- Most products have ratings between 3.5 and 5.0
- Products with very low ratings (< 2.5) are relatively rare
- This metric helps identify product quality from customer perspective

---

## Task 3: Top 10 Most Reviewed Products

### Objective

Identify the 10 products with the highest number of reviews.

### Methodology

Two-step MapReduce process:

- **Step 1:** Count reviews per product (same as Task 1)
- **Step 2:** Swap key-value pairs, send all to single reducer, sort and select top 10

### Analysis

- Top product has 6,340 reviews - significantly more than others
- Top 10 products range from 2,371 to 6,340 reviews
- These are likely popular or long-standing products in the grocery category
- Two-step approach ensures global sorting despite distributed processing

---

## Task 4: Average Helpfulness Score per Product

### Objective

Calculate the average helpfulness ratio (helpful_votes / total_votes) for each product.

### Methodology

- **Mapper:** Extracts `asin` and `helpful` field `[helpful_votes, total_votes]`, calculates ratio
- **Reducer:** Averages helpfulness scores across all reviews with votes

### Analysis

- Helpfulness scores range from 0.0 to 1.0
- Many products have high helpfulness scores (> 0.8), indicating quality reviews
- Only reviews with at least one vote are included in the calculation
- This metric helps identify products with useful customer feedback

---

## Task 5: Sentiment Analysis on Review Texts

### Objective

Perform text-based sentiment analysis to categorize reviews as positive, neutral, or negative based on review content.

### Methodology

- **Keyword-Based Classification:** Uses predefined positive and negative word lists
- **Mapper:** Analyzes `reviewText` and `summary` fields, counts sentiment keywords
- **Reducer:** Aggregates sentiment counts per product

### Analysis

- **Positive Sentiment Dominance:** Most products show predominantly positive reviews
- **Keyword Approach:** Simple but effective for basic sentiment classification
- **Performance:** Optimized using substring matching instead of regex for 1.3M reviews
- **Limitations:**
  - Doesn't account for negation ("not good")
  - Context-independent keyword matching
  - May miss sarcasm or complex sentiment
- **Future Improvements:** Could use ML-based sentiment models (VADER, TextBlob, or transformers)

---

## Technical Implementation Details

### MapReduce Framework

- **Library:** MRJob (Python MapReduce framework)
- **Execution Mode:** Local mode (single machine)
- **Data Format:** JSONL (JSON Lines) for efficient line-by-line processing

### Performance Optimizations

1. **Combiner Usage (Task 1):**
   - Reduces network traffic by pre-aggregating data locally
   - Particularly effective for counting operations

2. **Efficient JSON Parsing:**
   - Uses `json.loads()` for each line
   - Graceful error handling for malformed data

3. **Sentiment Analysis Optimization:**
   - Simple substring matching (`word in text`) instead of regex
   - Reduced keyword list to most impactful terms
   - Processes 1.3M reviews efficiently

### Error Handling

- All mappers use try-except blocks to skip malformed JSON
- Validates data existence before processing
- Gracefully handles missing fields

---

## Running the Code

### Prerequisites

```bash
pip install mrjob
```

### Execution Commands

```bash
# Task 1: Review Count
python3 mr_review_count.py grocery_reviews.json

# Task 2: Average Star Rating
python3 mr_avg_star_rating.py grocery_reviews.json

# Task 3: Top 10 Reviewed Products
python3 mr_top10_reviewed.py grocery_reviews.json

# Task 4: Average Helpfulness
python3 mr_avg_helpfulness.py grocery_reviews.json

# Task 5: Sentiment Analysis
python3 mr_sentiment_analysis.py grocery_reviews.json
```

### Output Redirection

```bash
# Save results to file
python3 mr_review_count.py grocery_reviews.json > review_counts.txt
```

---
