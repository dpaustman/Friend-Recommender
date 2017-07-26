from mrjob.job import MRJob
from mrjob.step import MRStep

class FriendsRecommender(MRJob):

    def steps(self):
        return [MRStep(mapper=self.map_input, combiner=self.count_number_of_friends),
                MRStep(mapper=self.count_max_of_mutual_friends, combiner=self.top_recommendations)]

    def map_input(self, key, line):
    
        input = line.split(";")
        user_id, item_ids = input[0], input[1:]
        
        for i in range(len(item_ids)):
            
            f1 = item_ids[i]
            
            if user_id < f1:
                yield (user_id, f1), -1
            else:
                yield (f1, user_id), -1

            for j in range(i + 1, len(item_ids)):
            
                f2 = item_ids[j]
            
                if f1 < f2:
                    yield (f1, f2), 1
                else:
                    yield (f2, f1), 1

    def count_number_of_friends(self, key, values):
        
        f1, f2 = key
        mutual_friends_count = 0
        for value in values:
            if value == -1:
                return
            mutual_friends_count += value

        yield (f1, f2), mutual_friends_count


    def count_max_of_mutual_friends(self, key, values):
        
        f1, f2 = key
        # for score in values:
        yield f1, (f2, int(values))
        yield f2, (f1, int(values))

    def top_recommendations(self, key, values):
        
        recommendations = []
        for idx, (item, score) in enumerate(values):
            recommendations.append((item, score))

        yield key, sorted(recommendations, key=lambda k: -k[1])[:50 ]


if __name__ == '__main__':
    FriendsRecommender.run()
