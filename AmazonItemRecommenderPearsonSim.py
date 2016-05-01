import scipy
import scipy.stats
from scipy.stats import pearsonr

from mrjob.job import MRJob
from mrjob.step import MRStep
import ast
from itertools import combinations
import math

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
        d = ast.literal_eval(line);
        yield d['reviewerID'], (d['asin'], float(d['overall']))
        
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
        
    def pearson_correlation_similarity(self, ratingPairs):
        # Computes the cosine similarity metric between two
        # rating vectors.
        numPairs = 0
        listX = []
        listY = []
        for ratingX, ratingY in ratingPairs:
            listX.append(ratingX)
            listY.append(ratingY)
            numPairs+=1
                     
        score = 0
        score = pearsonr(listX, listY)
        return (score[0], numPairs)
    
    def reducer_compute_similarity(self, ItemPair, ratingPairs):
        score, numPairs = self.pearson_correlation_similarity(ratingPairs)
	x=float(score)
        if (numPairs > 10 and not math.isnan(x) and score>0.5):
            yield ItemPair, (score, numPairs)

    def mapper_sort_similarities(self, ItemPair, scores):
        # Shuffle things around so the key is (movie1, score)
        # so we have meaningfully sorted results.
        score, n = scores
        item1, item2 = ItemPair

        yield (item1, score), \
            (item2, n)

    def reducer_output_similarities(self, itemScore, similarN):
        # Output the results.
        # Movie => Similar Movie, score, number of co-ratings
        item1, score = itemScore
        for item2, n in similarN:
            yield item1, (item2, score, n)
        
if __name__ == '__main__':
    MRItemCF.run()
