from IndexReader import *

import math

class IndexSearcher:
    __index_reader = None
    def __init__(self, iReader):
        """Constructor.
        iReader is the IndexReader object on which the
        search should be performed"""
        self.__index_reader = iReader


    def vectorSpaceSearch(self, query, k):
        """Returns a tuple containing the id-s of the
        k most highly ranked reviews for the given
        query, using the vector space ranking function
        lnn.ltc (using the SMART notation). The id-s
        should be sorted by the ranking."""

        q = [t.lower() for t in list(dict.fromkeys(query.split()))]

        #calculate ltc for query
        list_of_wtq = {}
        for term in q:
            list_of_wtq[term]= self.log_frequency_weighing_in_query(q,term)


        normal = self.calc_normal(list_of_wtq.values())
        if normal == 0: # the query does not exist in any document
            return 0


        for key in list_of_wtq.keys():
            list_of_wtq[key] = list_of_wtq[key]/normal

        #print(list_of_wtq)


        #calculate lnn for documents and calculate cosine scores
        scores = {} # cosine scores

        for term in q:
            freq_list = self.__index_reader.getDocsWithToken(term)
            wtq = list_of_wtq[term] # wei
            #going through the term posting list
            #and for each document in the posing list
            #   calculate score
            for i in range(0,len(freq_list),2):
                wtd = self.log_frequency_weighing_in_document(freq_list[i + 1]) # calculate lnn of document i.e. weight of term in document
                if scores.get(freq_list[i]):
                    scores[freq_list[i]] += wtd* wtq  #[0] += wtd* wtq
                    #scores[freq_list[i]][1] += wtd**2 # to calculate the length of the document
                else:
                    scores[freq_list[i]] = self.log_frequency_weighing_in_document(freq_list[i+1]) * wtq#[self.log_frequency_weighing_in_document(freq_list[i+1]) * wtq, wtd**2]

        #for key in scores.keys():
        #    scores[key] = scores[key][0]/math.sqrt(scores[key][1]) # score/length

        sorted_scores = sorted(scores.items(), key=lambda x: x[1],reverse=True)

        top_k_scores = []
        count=0
        for tup in sorted_scores:
            if count > k:
                break
            top_k_scores.append(tup[0])
            #print(tup)
            count+=1

        return tuple(top_k_scores)



    #######
    # helper methods
    #######
    def log_frequency_weighing_in_document(self,freq_in_doc):
        #calculates lnn
        if freq_in_doc == 0:
            return 0
        else:
            return 1 + math.log(freq_in_doc,10)

    #######
    def log_frequency_weighing_in_query(self, query, term):
        #calculates lt (logarithmic term frequency and idf document frequency )
        log_term_frequency_in_query = 1 +  math.log(query.count(term), 10) # term appears at least once in the query
        df = self.__index_reader.getTokenFrequency(term)
        if df == 0:
            idf = 0
        else:
            idf = math.log(self.__index_reader.getNumberOfDocuments()/df  ,10)

        return log_term_frequency_in_query * idf

    #######
    def calc_normal(self,list_of_wt):
        sum_of_sqrs = 0
        for i in list_of_wt:
            sum_of_sqrs += i**2
        return math.sqrt(sum_of_sqrs)




###########################
###########################


'''

indexR = IndexReader('index_blocks/')

searcher = IndexSearcher(indexR)

#searcher.vectorSpaceSearch("i love fish",5)

score =searcher.vectorSpaceSearch("i",5)
print(score)


'''


