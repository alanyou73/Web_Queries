from Dictionary import *
from PostingList import *
from IndexWriter import *


class IndexReader:

    def __init__(self, dir):
        """Creates an IndexReader which will read from
        the given directory
        dir is the name of the directory in which all
        index files are located."""


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
          c.read index_blocks/location_pointer_to_posting_freq_lists_in_inverted_index.txt
        3.turn all 3 back to a list
        4. recreate self.__dictionary = {}
        
          
           
        """





    def getTokenFrequency(self, token):
        """Return the number of documents containing a
        given token (i.e., word)
        Returns 0 if there are no documents containing
        this token"""
    def getTokenCollectionFrequency(self, token):
        """Return the number of times that a given
        token (i.e., word) appears in the whole
        collection.
        Returns 0 if there are no documents containing
        this token"""
    def getDocsWithToken(self, token):
        """Returns a series of integers of the form id1, freq-1, id-2, freq-2, ... such
        that id-n is the n-th document containing the
        given token and freq-n is the
        number of times that the token appears in doc
        id-n
        Note that the integers should be sorted by id.
        Returns an empty Tuple if there are no
        documents containing this token"""
    def getNumberOfDocuments(self):
        """Return the number of documents in the
        collection"""