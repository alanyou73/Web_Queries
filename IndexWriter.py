import os
import sys
import collections
import ast
import re
import shutil
from array import array

from Dictionary import *
from PostingList import *

from os import listdir
from collections import OrderedDict
import time

from datetime import datetime


class IndexWriter:
    __block_size_limit = 10737418240 # 10GB

    #__block_size_limit = 4294967296  # 4GB

    __dir = ""
    __dictionary = {}
    __blocksDictionaries = {}
    __number_of_documents = 0
    ##  FOR TESTING PURPOSES   ##
    __size_of_dic = 0

    ####

    def __init__(self, inputFile, dir):

        """Given a collection of documents, creates an on disk index
        inputFile is the path to the file containing the review data (the path includes the filename itself)
        dir is the name of the directory in which all index files will be created
        if the directory does not exist, it should be created"""

        ###for testing purposes
        start = str(datetime.now().ctime())
        print("starting time : " + start)

        self.__dir = dir
        if not os.path.exists(dir):
            os.mkdir(dir)
        if os.path.exists(dir + 'spimi_inverted_index.txt'):
            os.remove(dir + 'spimi_inverted_index.txt')

        # file = open(inputFile, "r")
        with open(inputFile, 'r') as file:
            self.SPIMI_Invert(file)
        # file.close()

        # print(self.__dictionary)
        print("\ntable size is : {} megabytes ".format(self.__size_of_dic * 10 ** (-6)))

        ################
        print("=============== Merging SPIMI blocks into final inverted index... ===============")
        self.merge(dir)

        # print(self.__dictionary)
        # print("\ntable size is : {} megabytes ".format(sys.getsizeof(self.__dictionary)*10**(-6)))
        # print(self.__blocksDictionaries)
        # print("\nspimi size is : {} kilobytes ".format(sys.getsizeof(self.__blocksDictionaries.)))##*10**(-6)))

        os.rename(dir + "block-0.txt", dir + "spimi_inverted_index.txt")  # renaming block-0

        os.rename(dir + "posting_lists_size_in_inverted_index-block-0.txt",dir + "posting_lists_size_in_inverted_index.txt")  # renaming block-0
        os.rename(dir + "frequencies_lists_size_in_inverted_index-block-0.txt",dir + "frequencies_lists_size_in_inverted_index.txt")  # renaming block-0
        os.rename(dir + "location_pointer_to_posting_freq_lists_in_inverted_index-block-0.txt",dir + "location_pointer_to_posting_freq_lists_in_inverted_index.txt")  # renaming block-0

        print("completed spimi at : ", datetime.now().ctime())

        print("SPIMI completed! All blocks merged into final index: spimi_inverted_index.txt")
        final_dic = self.__blocksDictionaries.popitem()  ## self.__blocksDictionaries is emptied
        self.__dictionary = final_dic[1]

        dict = list(self.__dictionary.keys())
        # print(dict)

        compressed_dictionary = Dictionary(dict, "STR")
        # create a file and write compressed_dictionary.GetString() in it (on disk)
        text_file = open(dir + "compressed_dictionary.txt", "w")
        print(" -- Writing  : compressed_dictionary ...")
        text_file.write(compressed_dictionary.GetString())
        text_file.close()
        ##
        # turn compressed_dictionary.GetTable() into byte array table_byte_array
        table_byte_array = VBEncode(compressed_dictionary.GetTable())
        write_byte_array_on_disk(table_byte_array, dir, "table_byte_array")

        ## write number of documents on disk
        text_file = open(dir + "number_of_documents.txt", "w")
        print(" -- Writing  : number_of_documents ...")
        text_file.write(str(self.__number_of_documents))
        text_file.close()

        print("starting time was : " + start)
        print("ending time : ", datetime.now().ctime())

        print("=============== INVERTED INDEX FILES ARE ON DISK ===============")
        print("=============== INVERTED INDEX BUILD COMPLETED ===============")

    #####################################
    #####################################
    def SPIMI_Invert(self, file):
        """ Applies the Single-pass in-memory indexing algorithm """

        block_number = 0
        dictionary = {}  # (term - postings list)
        docID = 0
        # size_of_block = sys.getsizeof(dictionary)

        size_of_block = 0

        # f = file.read(1000000).split()
        # print(f)
        for doc in file:
            # print(doc)
            if doc == '*' * 80 + '\n':  #
                docID += 1
            else:
                # print(docID)
                docAsList = doc.split()  # convert the document a list of strings

                term = ''
                for word in docAsList:
                    if word.isalnum():  # word is alphanumeric
                        term = word.lower()
                    else:  # word is NOT alphanumeric
                        tempWord = ''
                        for char in word:
                            if char.isalnum():
                                tempWord += char
                            elif tempWord != '':
                                term = tempWord.lower()
                                ## new Term ##
                                # If term occurs for the first time
                                if term not in dictionary:
                                    dictionary[term] = [docID]  # Add term to dictionary, create new postings list, and add docID
                                    # size_of_block += sys.getsizeof(term) + sys.getsizeof([]) + sys.getsizeof(docID)
                                    size_of_block += sys.getsizeof(term) + sys.getsizeof(docID)
                                else:  # If term has a subsequent occurence
                                    dictionary[term].append(
                                        docID)  # Add a posting (docID) to the existing posting list of the term
                                    size_of_block += sys.getsizeof(docID)

                                tempWord = ''
                        if tempWord != '':
                            term = tempWord.lower()
                    ## new Term ##
                    # If term occurs for the first time
                    if term not in dictionary:
                        dictionary[term] = [docID]  # Add term to dictionary, create new postings list, and add docID

                        # size_of_block += sys.getsizeof(term) + sys.getsizeof([]) + sys.getsizeof(docID)

                        size_of_block += sys.getsizeof(term) + sys.getsizeof(docID)

                        # if term not in self.__dictionary:
                        #    self.__dictionary[term] = []
                    else:  # If term has a subsequent occurence
                        dictionary[term].append(docID)  # Add a posting (docID) to the existing posting list of the term
                        size_of_block += sys.getsizeof(docID)

                if size_of_block > self.__block_size_limit:
                    print("\nblock {} size is : {} megabytes ".format(block_number, size_of_block * 10 ** (-6)))

                    self.__blocksDictionaries[block_number] = self.create_block_dictionary(dictionary,block_number)  # create block dictionary and write block of posting list and frequencies to disk

                    block_number += 1
                    dictionary = {}

                    # size_of_block = sys.getsizeof(dictionary)

                    size_of_block = 0

        if size_of_block > sys.getsizeof(dictionary):  # if dictionary's size is smaller than block size i.e. last block
            print("\nblock size is : {} megabytes ".format(block_number, size_of_block * 10 ** (-6)))

            self.__blocksDictionaries[block_number] = self.create_block_dictionary(dictionary,
                                                                                   block_number)  # create block dictionary and write block of posting list and frequencies to disk

        self.__number_of_documents = docID
        # self.__dictionary = sorted(self.__dictionary)

    ##################################
    ##################################
    def create_block_dictionary(self, term_postings_list, block_number):

        """
        :param term_postings_list: a dictionary , keys are the terms , value is posting list with duplicates
        :param block_number: number of block
        :return: block dictionary where the key is the term
        and value is a dictionary with keys and values as follows, "sizePL" : posting list size in bytes
                                                                   "sizeFreq" : term frequency list
                                                                   "location" : pointer to the location of the posting list in the block on disk
        """
        print("-- Creating block number {} ...".format(block_number))
        print("creation block starting time : " + datetime.now().ctime())
        block_dictionary = {}
        blockByteArray = bytearray()
        sorted_terms = sorted(term_postings_list)
        pointer_to_postinglist_freqlist = 0
        for term in sorted_terms:
            # print(term)

            block_dictionary[term] = self.block_to_byte_array(term_postings_list[term])

            block_dictionary[term]["location"] = pointer_to_postinglist_freqlist  # location pointer
            pointer_to_postinglist_freqlist = block_dictionary[term]["location"] + block_dictionary[term]["sizePL"] + block_dictionary[term]["sizeFreq"]
            blockByteArray += block_dictionary[term]["pl_freq_byteArray"]  ## append current byte array to block array
            block_dictionary[term].pop("pl_freq_byteArray")

        '''
        ########       TESTING SIZE OF BLOCK DICTIONARY             ##########
        size_of_block = sys.getsizeof({})
        for term in block_dictionary.items():
            #print(term[0])
            size_of_block += sys.getsizeof(term[0]) + sys.getsizeof(term[1]["sizePL"]) + sys.getsizeof(term[1]["sizeFreq"]) + sys.getsizeof(term[1]["location"])
            size_of_block += sys.getsizeof("sizePL") +sys.getsizeof("sizeFreq") +sys.getsizeof("location")
            size_of_block += sys.getsizeof({})
        #print(block_dictionary)
        self.__size_of_dic+=size_of_block
        size = size_of_block*10**(-6)
        print("\nblock_dictionary {} size is : {} megabytes ".format(block_number, size ))
        ########    END   TESTING SIZE OF BLOCK DICTIONARY             ##########

        '''

        ###write block to disk
        self.write_byte_array_block_to_disk(blockByteArray, block_number)
        ###############

        ## write block pointers to disk
        self.write_block_pointers_to_disk(block_dictionary, block_number)

        print("creation block ending time : " + datetime.now().ctime())

        return block_dictionary

    ###################################
    def block_to_byte_array(self, pl_with_duplicates):
        """ creating posting list and frequency list as byte array,
            creates , blockPtrs["pl_freq_byteArray"] ,   blockPtrs["sizePL"] , blockPtrs["sizeFreq"]
            and returns blockPtrs
        """
        blockPtrs = {}
        counter = collections.Counter(pl_with_duplicates)
        currPostingList = []
        frequencyList = []
        for docId in counter.keys():
            currPostingList.append(docId)
            frequencyList.append(counter[docId])

        ##compressing posting list  i.e.  list of docIDs
        copmressedCurrPLClass = PostingList(currPostingList, "V")
        copmressedCurrPL = copmressedCurrPLClass.GetList()
        blockPtrs["pl_freq_byteArray"] = copmressedCurrPL
        blockPtrs["sizePL"] = copmressedCurrPL.__len__()  # size of bytearray content in bytes

        ##compressing frequencyList i.e. frequency of term in each docID
        copmressedFreq = VBEncode(frequencyList)
        blockPtrs["pl_freq_byteArray"] += copmressedFreq
        blockPtrs["sizeFreq"] = copmressedFreq.__len__()  # size of bytearray content in bytes

        #######################
        #
        ######################

        # print(pl_with_duplicates)
        # print(blockPtrs)
        return blockPtrs

    #################################################
    #################################################
    def write_byte_array_block_to_disk(self, block_as_byte_array, block_number):
        """ Writes posting list and frequency list  byte array to disk """
        # Define block
        base_path = self.__dir  # 'index_blocks/'
        block_name = 'block-' + str(block_number) + '.txt'

        currBlock = open(base_path + block_name, 'wb')

        print(" -- Writing list block: " + block_name + "...")
        currBlock.write(block_as_byte_array)
        currBlock.close()

    #############################################################
    #############################################################

    def write_block_pointers_to_disk(self, block_dictionary, block_number):
        """
        :param block_dictionary:
        :param block_number:
        :return:
        """
        # turn  "sizePL" ,"sizeFreq" ,"location" of each term in __dictionary into a separate list
        posting_lists_size_in_spimi_inverted_index = []
        frequencies_lists_size_in_spimi_inverted_index = []
        location_pointer_to_posting_freq_lists_in_spimi_inverted_index = []
        for ptr in block_dictionary.values():
            posting_lists_size_in_spimi_inverted_index.append(ptr["sizePL"])
            frequencies_lists_size_in_spimi_inverted_index.append(ptr["sizeFreq"])
            location_pointer_to_posting_freq_lists_in_spimi_inverted_index.append(ptr["location"])

        ## turn all three lists to byte array
        posting_lists_size_in_spimi_inverted_index_byte_array = VBEncode(posting_lists_size_in_spimi_inverted_index)
        frequencies_lists_size_in_spimi_inverted_index_byte_array = VBEncode(frequencies_lists_size_in_spimi_inverted_index)
        location_pointer_to_posting_freq_lists_in_spimi_inverted_index_to_gaps = calculateGaps(location_pointer_to_posting_freq_lists_in_spimi_inverted_index)
        location_pointer_to_posting_freq_lists_in_spimi_inverted_index_byte_array = VBEncode(location_pointer_to_posting_freq_lists_in_spimi_inverted_index_to_gaps)

        # write posting_lists_size_in_spimi_inverted_index_byte_array in it (on disk)

        write_byte_array_on_disk(posting_lists_size_in_spimi_inverted_index_byte_array, self.__dir,"posting_lists_size_in_inverted_index-block-{}".format(block_number))
        # write frequencies_lists_size_in_spimi_inverted_index_byte_array in it (on disk)
        write_byte_array_on_disk(frequencies_lists_size_in_spimi_inverted_index_byte_array, self.__dir,"frequencies_lists_size_in_inverted_index-block-{}".format(block_number))
        # write location_pointer_to_posting_freq_lists_in_spimi_inverted_index_byte_array in it (on disk)
        write_byte_array_on_disk(location_pointer_to_posting_freq_lists_in_spimi_inverted_index_byte_array, self.__dir,"location_pointer_to_posting_freq_lists_in_inverted_index-block-{}".format(block_number))

        ###################

        for term in block_dictionary.keys():
            block_dictionary[term] = 0  # remove pointers dictionary

    ################################
    ################################

    def get_block_pointers_from_disk(self, block_number):
        print("-- Recreating pointers for block number {} from disk...".format(block_number))
        posting_lists_size_in_inverted_index = read_bytes_from_disk(self.__dir + "posting_lists_size_in_inverted_index-block-{}.txt".format(block_number))  #
        frequencies_lists_size_in_inverted_index = read_bytes_from_disk(self.__dir + "frequencies_lists_size_in_inverted_index-block-{}.txt".format(block_number))  #
        location_pointer_to_posting_freq_lists_in_inverted_index_gaps = read_bytes_from_disk(self.__dir + "location_pointer_to_posting_freq_lists_in_inverted_index-block-{}.txt".format(block_number))  #
        location_pointer_to_posting_freq_lists_in_inverted_index = unGap(location_pointer_to_posting_freq_lists_in_inverted_index_gaps)

        os.remove(self.__dir + "posting_lists_size_in_inverted_index-block-{}.txt".format(block_number))
        os.remove(self.__dir + "frequencies_lists_size_in_inverted_index-block-{}.txt".format(block_number))
        os.remove(self.__dir + "location_pointer_to_posting_freq_lists_in_inverted_index-block-{}.txt".format(block_number))

        for i, key in enumerate(self.__blocksDictionaries[block_number].keys()):
            self.__blocksDictionaries[block_number][key] = {}
            self.__blocksDictionaries[block_number][key]["sizePL"] = posting_lists_size_in_inverted_index[i]
            self.__blocksDictionaries[block_number][key]["sizeFreq"] = frequencies_lists_size_in_inverted_index[i]
            self.__blocksDictionaries[block_number][key]["location"] = location_pointer_to_posting_freq_lists_in_inverted_index[i]

    #############################################################
    #############################################################

    def convert_bytes_block(self, block_number):
        """
        given a block number, converts an entire block of bytes from disk into,posting-frequency list
        and returns a block_dictionary with term as key and ["posting_list .... , frequency_list.... ]  as value
        :param block_number:
        :return: block_dictionary
        """

        block_dictionary = {}
        print("-- Converting bytes from block...")
        # print(block_number)
        path = self.__dir + 'block-' + str(block_number) + '.txt'

        ###################

        # GET POINTERS FROM DISK
        self.get_block_pointers_from_disk(block_number)

        # print(self.__blocksDictionaries)
        d = self.__blocksDictionaries.pop(block_number)  # pointers
        # print(d)
        for item in d.items():
            # print(item[0]) # term
            # print(item[1]) # pointers
            block_dictionary[item[0]] = read_bytes_posting_list_from_disk(path, item[1])  ## ["posting_list .... , frequency_list.... ]
            # print(posting_frequency_list)

        # print(block_dictionary)
        return block_dictionary

    #############################################################
    #############################################################
    #############################################################
    def merge(self, dir):
        """ Merges SPIMI blocks into final inverted index """
        merge_completed = False
        # spimi_index = open(dir + 'spimi_inverted_index.txt', 'a+')

        # for key in self.__blocksDictionaries.keys():
        #   print(key)

        while len(self.__blocksDictionaries.keys()) > 1:
            tempBlocksDictionary = {}
            lastBlockNum = 0
            for i in range(int(len(self.__blocksDictionaries.keys()) / 2)):
                tempBlocksDictionary[i] = self.merge_two_blocks(i, max(self.__blocksDictionaries.keys()))  # merge first and last keys
                lastBlockNum = i

            ######
            # add remaining block to tempBlocksDictionary if number of blocks is odd
            if len(self.__blocksDictionaries.keys()) % 2 != 0:
                remaining_block = self.__blocksDictionaries.popitem()
                tempBlocksDictionary[lastBlockNum + 1] = remaining_block[1]

            # now self.__blocksDictionaries is empty
            ######
            self.__blocksDictionaries = tempBlocksDictionary
        ####
        # inverted index is block number 0
        ####

    ####################################
    def merge_two_blocks(self, blkNumA, blkNumB):
        """
        given numbers of two blocks creates a merged block of the two on disk
        and returns its dictionary
        :param blkNumA:
        :param blkNumB:
        :return: block_dictionary like this one {term: {'sizePL': x, 'sizeFreq': y, 'location': z},...,...}
        """
        print("-- merging block {} with block {}...\n".format(blkNumA, blkNumB))
        block_dictionary = {}

        a_block_dictionary = self.convert_bytes_block(blkNumA)  #
        b_block_dictionary = self.convert_bytes_block(blkNumB)  #

        while len(a_block_dictionary) != 0:
            item_a = a_block_dictionary.popitem()
            if b_block_dictionary.get(item_a[0]):
                item_b = b_block_dictionary.pop(item_a[0])
                block_dictionary[item_a[0]] = self.merge_term_postings_list(item_a[1],item_b)  # merge term's posting lists
            else:  # no more items in b_block_dictionary
                item_pl = item_a[1][:int(len(item_a[1]) / 2)]
                item_freq = item_a[1][int(len(item_a[1]) / 2):]
                block_dictionary[item_a[0]] = self.posting_list_with_duplicates(item_pl, item_freq)

        while len(b_block_dictionary) != 0:  # there are more items in b_block_dictionary
            item_b = b_block_dictionary.popitem()
            item_pl = item_b[1][:int(len(item_b[1]) / 2)]
            item_freq = item_b[1][int(len(item_b[1]) / 2):]
            block_dictionary[item_b[0]] = self.posting_list_with_duplicates(item_pl, item_freq)

        '''
        after retrieving both posting_frequencies_list from disk
        delete both from disk
        the merged block will be saved as the block number of blockA
        '''
        ## delete both blocks from disk
        self.delete_block(blkNumA)
        self.delete_block(blkNumB)
        ##write new merged block on disk
        block_dictionary_with_pointers = self.create_block_dictionary(block_dictionary,blkNumA)  # the merged block will be saved as the block number of blockA
        return block_dictionary_with_pointers

    #############################################################
    #############################################################
    #############################################################

    def merge_term_postings_list(self, a_postings_frequencies, b_postings_frequencies):
        """
         helper method for merge_two_blocks, gets 2 postings_frequencies list of a term
         and returns a merged posting_list_with_duplicates list
        :param a_postings_frequencies: posting list and frequencies list of block a
        :param b_postings_frequencies: posting list and frequencies list of block b
        :return: posting_list_with_duplicates
        """
        #####
        a_pl = a_postings_frequencies[:int(len(a_postings_frequencies) / 2)]
        a_freq = a_postings_frequencies[int(len(a_postings_frequencies) / 2):]
        a_posting_list_with_duplicates = self.posting_list_with_duplicates(a_pl, a_freq)
        #####
        b_pl = b_postings_frequencies[:int(len(b_postings_frequencies) / 2)]
        b_freq = b_postings_frequencies[int(len(b_postings_frequencies) / 2):]
        b_posting_list_with_duplicates = self.posting_list_with_duplicates(b_pl, b_freq)
        ######
        return sorted(a_posting_list_with_duplicates + b_posting_list_with_duplicates)

    #########################
    def posting_list_with_duplicates(self, postings_list, frequencies_list):
        """
        given a postings_list and a frequencies_list returns one postings list with duplicates
        :param postings_list:
        :param frequencies_list:
        :return: posting_list_with_duplicates
        """
        posting_list_with_duplicates = []
        for i in range(len(postings_list)):
            for j in range(frequencies_list[i]):
                posting_list_with_duplicates.append(postings_list[i])

        return posting_list_with_duplicates

    ##########################
    ##########################

    def delete_block(self, block_number):
        """
        given a block number removes block from disk
        :param block_number:
        :return:
        """
        print("-- deleting block number {} ...".format(block_number))
        path = self.__dir + 'block-' + str(block_number) + '.txt'
        os.remove(path)

    #############################################################

    def removeIndex(self, dir):
        """Delete all index files by removing the given directory
        dir is the name of the directory in which all index files are located. After removing the files, the directory should be deleted."""
        self.deleteblocks()
        os.rmdir(dir)

    def deleteblocks(self):
        """ Delete directories and files under ./index_blocks"""
        for root, directories, files in os.walk('./' + self.__dir):
            for f in files:
                os.unlink(os.path.join(root, f))
            for directory in directories:
                shutil.rmtree(os.path.join(root, directory))


#################################################
#################################################

def read_bytes_posting_list_from_disk(block_path_and_directory, pointers):
    """
    reads posting list of term from disk and converts it to list of docIDs and frequencies
    :param block_path_and_directory: name and directory
    :param pointers: dictionary with with keys :  'sizePL', 'sizeFreq', 'location'
    :return: postings list followed by frequency as a list()
    """
    # Seek can be called one of two ways:
    #   x.seek(offset)
    #   x.seek(offset, starting_point)

    # starting_point can be 0, 1, or 2
    # 0 - Default. Offset relative to beginning of file
    # 1 - Start from the current position in the file
    # 2 - Start from the end of a file (will require a negative offset)

    with open(block_path_and_directory, "rb") as binary_file:
        # Seek a specific position in the file and read N bytes
        binary_file.seek(pointers["location"], 1)  # Start from the current position in the file
        bytes_posting_list = binary_file.read(pointers["sizePL"])
        posting_list_gaps = VBDecode(bytes_posting_list)
        posting_and_frequencies_list = unGap(posting_list_gaps)
        bytes_frequencies = binary_file.read(pointers["sizeFreq"])
        posting_and_frequencies_list += VBDecode(bytes_frequencies)
    return posting_and_frequencies_list


#################################################
#################################################


def write_byte_array_on_disk(byte_array, dir, file_name):
    """
    given a byte array and a file name, creates a file named file_name and writes byte array in it
    :param byte_array:
    :param file_name:
    :return:
    """
    # Define block
    path = dir  # 'index_blocks/'
    file = file_name + '.txt'

    curr_file = open(path + file, 'wb')

    print(" -- Writing bytes file : " + file_name + " ...")
    curr_file.write(byte_array)
    curr_file.close()


#####################

def read_bytes_from_disk(directory_and_path):
    with open(directory_and_path, "rb") as binary_file:
        bytes_array = binary_file.read()
        array = VBDecode(bytes_array)
    # os.remove(directory_and_path)
    return array


#####################
###################        TESTING               #########################


# alanspath="C:/Users/alany/OneDrive/Desktop/files/1000.txt"
#mypath = '/Users/yairwygoda/Desktop/100000.txt'
#indexW = IndexWriter(mypath ,'index_blocks/')
# print("deleting index files and directory")
# indexW.removeIndex('index_blocks/')


'''
with open("index_blocks/block-0.txt", "rb") as binary_file:
    # Seek a specific position in the file and read N bytes
    binary_file.seek(0, 0)  # Go to beginning of the file
    couple_bytes = binary_file.read(2)
    print(couple_bytes)
'''