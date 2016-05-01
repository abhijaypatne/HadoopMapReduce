from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

from itertools import combinations

class MRItemCF(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                    reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,
                    reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities,
                    reducer=self.reducer_output_similarities)]
    
    def mapper_parse_input(self, key, line):
        (reviewerID,asin,overall) = line.split(',')
        yield reviewerID, (asin, float(overall))
        
    def reducer_ratings_by_user(self, reviewerID, itemRatings):
        ratings= []
        for asin, overall in itemRatings:
            ratings.append((asin, overall))
        yield reviewerID, ratings
        
    def mapper_create_item_pairs(self, user_id, itemRatings):
        for itemRating1, itemRating2 in combinations(itemRatings, 2):
            asin1 = itemRating1[0]
            rating1 = itemRating1[1]
            asin2 = itemRating2[0]
            rating2 = itemRating2[1]

            # Produce both orders so sims are bi-directional
            yield (asin1, asin2), (rating1, rating2)
            yield (asin2, asin1), (rating2, rating1)
        
    def cosine_similarity(self, ratingPairs):
        # Computes the cosine similarity metric between two
        # rating vectors.
        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))

        return (score, numPairs)
    
    def reducer_compute_similarity(self, ItemPair, ratingPairs):
        score, numPairs = self.cosine_similarity(ratingPairs)
        
        if (numPairs > 10 and score > 0.95):
            yield ItemPair, (score, numPairs)
        
        """yield ItemPair, (score, numPairs)"""

    def mapper_sort_similarities(self, ItemPair, scores):
        score, n = scores
        item1, item2 = ItemPair

        yield (item1, score), \
            (item2, n)

    def reducer_output_similarities(self, itemScore, similarN):
        item1, score = itemScore
        for item2, n in similarN:
            yield item1, (item2, score, n)
        
if __name__ == '__main__':
    MRItemCF.run()