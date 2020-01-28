from Dictionary import *
from PostingList import *
from IndexWriter import *


class IndexReader:

    __dictionary = {}
    __dir = ""
    __inverted_index_path = ""
    __number_of_documents = 0
    def __init__(self, dir):
        """Creates an IndexReader which will read from
        the given directory
        dir is the name of the directory in which all
        index files are located."""
        self.__dir = dir
        f = open(dir + "number_of_documents.txt", "r")
        numOfDocs = f.read()
        self.__number_of_documents = int(numOfDocs)
        self.__inverted_index_path = dir + "spimi_inverted_index.txt"
        self.recreate_dictionary() # reconstitutes self.__dictionary from disk
        #print(self.__dictionary.keys())



        """
        recreate self.__dictionary = {}
        where : the keys are the terms from "index_blocks/compressed_dictionary.txt"
                the value is dictionary where keys are : "sizePL" : posting list size in bytes
                                                         "sizeFreq" : term frequency list
                                                         "location" : pointer to the location of the posting list in the block on disk
       
        steps:
        1.a.read compressed_dictionary from disk # 
          b.read table_byte_array.txt from disk  # x = read_bytes_from_disk("index_blocks/table_byte_array.txt")
          c.turn it into array
          d.decompress compressed_dictionary # get_terms_list(compressed_dictionary,x)
        
        2.a.read index_blocks/posting_lists_size_in_inverted_index.txt and 
          b.read index_blocks/frequencies_lists_size_in_inverted_index.txt
          c.read index_blocks/location_pointer_to_posting_freq_lists_in_inverted_index.txt AND UN-GAP IT!!!!!!!!!!!!!!!!!
        3.turn all 3 back to a list
        4. recreate self.__dictionary = {}
        
          
           
        """
    ##################################################
    ##################################################
    def recreate_dictionary(self):
        """
        decompress dictionary and inverted index pointers from disk an stores it in self.__dictionary
        :return: None
        """
        print("=============== RECREATING DICTIONARY FROM DISK ===============")
        #open and read compressed_dictionary from disk
        f = open(self.__dir + "compressed_dictionary.txt","r")
        compressed_dictionary = f.read()
        #retreive dictionary's table from disk
        table_byte_array = read_bytes_from_disk(self.__dir + "table_byte_array.txt")
        dict = get_terms_list(compressed_dictionary,table_byte_array) # decompress compressed_dictionary

        print("=============== RECREATING POINTERS FOR INVERTED INDEX FROM DISK ===============")
        posting_lists_size_in_inverted_index = read_bytes_from_disk(self.__dir + "posting_lists_size_in_inverted_index.txt") #
        frequencies_lists_size_in_inverted_index = read_bytes_from_disk(self.__dir + "frequencies_lists_size_in_inverted_index.txt") #
        location_pointer_to_posting_freq_lists_in_inverted_index_gaps = read_bytes_from_disk(self.__dir + "location_pointer_to_posting_freq_lists_in_inverted_index.txt") #
        location_pointer_to_posting_freq_lists_in_inverted_index = unGap(location_pointer_to_posting_freq_lists_in_inverted_index_gaps)

        for i in range(len(dict)):
            self.__dictionary[dict[i]] = {}
            self.__dictionary[dict[i]]["sizePL"] = posting_lists_size_in_inverted_index[i]
            self.__dictionary[dict[i]]["sizeFreq"] = frequencies_lists_size_in_inverted_index[i]
            self.__dictionary[dict[i]]["location"] = location_pointer_to_posting_freq_lists_in_inverted_index[i]


    ##################################################
    ##################################################



    def getTokenFrequency(self, token):
        """Return the number of documents containing a
        given token (i.e., word)
        Returns 0 if there are no documents containing
        this token"""
        if self.__dictionary.get(token) == None :
            return 0

        posting_and_frequencies_list = read_bytes_posting_list_from_disk(self.__inverted_index_path,self.__dictionary[token])
        posting_list = posting_and_frequencies_list[:int(len(posting_and_frequencies_list)/2)]
        return len(posting_list)

    def getTokenCollectionFrequency(self, token):
        """Return the number of times that a given
        token (i.e., word) appears in the whole
        collection.
        Returns 0 if there are no documents containing
        this token"""
        if self.__dictionary.get(token) == None :
            return 0

        posting_and_frequencies_list = read_bytes_posting_list_from_disk(self.__inverted_index_path,self.__dictionary[token])
        frequencies_list = posting_and_frequencies_list[int(len(posting_and_frequencies_list)/2):]
        return sum(frequencies_list)

    def getDocsWithToken(self, token):
        """Returns a series of integers of the form id1, freq-1, id-2, freq-2, ... such
        that id-n is the n-th document containing the
        given token and freq-n is the
        number of times that the token appears in doc
        id-n
        Note that the integers should be sorted by id.
        Returns an empty Tuple if there are no
        documents containing this token"""
        post_frequency_list = []
        if self.__dictionary.get(token) == None :
            return tuple(post_frequency_list)

        posting_and_frequencies_list = read_bytes_posting_list_from_disk(self.__inverted_index_path,self.__dictionary[token])
        posting_list = posting_and_frequencies_list[:int(len(posting_and_frequencies_list)/2)]
        frequencies_list = posting_and_frequencies_list[int(len(posting_and_frequencies_list)/2):]

        for i in range(int(len(posting_and_frequencies_list)/2)):
            post_frequency_list.append(posting_list[i])
            post_frequency_list.append(frequencies_list[i])

        return tuple(post_frequency_list)
    ################################################
    def getNumberOfDocuments(self):
        """Return the number of documents in the
        collection"""
        return self.__number_of_documents

####################################


#indexR = IndexReader('index_blocks/')

#print("number of documents is : ", indexR.getNumberOfDocuments())
'''

indexR = IndexReader('index_blocks/')

print(indexR.getDocsWithToken("going"))

print(indexR.getTokenFrequency("going"))

print(indexR.getTokenCollectionFrequency("going"))

print("number of documents is : ", indexR.getNumberOfDocuments())

'''





