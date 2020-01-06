from Dictionary import *
from PostingList import *
from IndexWriter import *


class IndexReader:

    def __init__(self, dir):
        """Creates an IndexReader which will read from
        the given directory
        dir is the name of the directory in which all
        index files are located."""






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