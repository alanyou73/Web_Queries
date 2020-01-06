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




###(location in str, pointer to the pl , length of pl? , freq , in how many documents  )



class IndexWriter:
    ##using a 5 MB blcok size
    __block_size_limit = 5000000  # 5MB

    #__block_size_limit = 750000  # default block size (in bytes) 0.75 MB
    #__block_size_limit = 100000  # 100 KB
    #__block_size_limit = 50000  # 50 KB
    #__block_size_limit = 10000  # 10 KB

    __dir =""
    __dictionary = {}
    __blocksDictionaries ={}
    def __init__(self, inputFile, dir):

        """Given a collection of documents, creates an on disk index
        inputFile is the path to the file containing the review data (the path includes the filename itself)
        dir is the name of the directory in which all index files will be created
        if the directory does not exist, it should be created"""
        self.__dir = dir
        if not os.path.exists(dir):
            os.mkdir(dir)
        if os.path.exists(dir + 'spimi_inverted_index.txt'):
            os.remove(dir + 'spimi_inverted_index.txt' )

        file = open(inputFile, "r")
        self.SPIMI_Invert(file)

        #print(self.__dictionary)
        print("\ntable size is : {} megabytes ".format(sys.getsizeof(self.__dictionary)*10**(-6)))

        ################
        print("=============== Merging SPIMI blocks into final inverted index... ===============")
        self.merge(dir)


        #print(self.__dictionary)
        #print("\ntable size is : {} megabytes ".format(sys.getsizeof(self.__dictionary)*10**(-6)))
        #print(self.__blocksDictionaries)
        #print("\nspimi size is : {} kilobytes ".format(sys.getsizeof(self.__blocksDictionaries.)))##*10**(-6)))

        os.rename(r'index_blocks/block-0.txt', r'index_blocks/spimi_inverted_index.txt')# renaming block-0
        print("SPIMI completed! All blocks merged into final index: spimi_inverted_index.txt")
        final_dic = self.__blocksDictionaries.popitem()## self.__blocksDictionaries is emptied
        self.__dictionary = final_dic[1]



        dict = list(self.__dictionary.keys())
        print(dict)

        compressed_dictionary = Dictionary(dict, "STR")
        #create a file and write compressed_dictionary.GetString() in it (on disk)
        text_file = open("index_blocks/compressed_dictionary.txt", "w")
        print(" -- Writing  : compressed_dictionary ...")
        text_file.write(compressed_dictionary.GetString())
        text_file.close()
        ##
        # turn compressed_dictionary.GetTable() into byte array table_byte_array
        table_byte_array = VBEncode(compressed_dictionary.GetTable())
        write_byte_array_on_disk(table_byte_array , "table_byte_array")

        ###TESTING####
        #x = read_bytes_from_disk("index_blocks/table_byte_array.txt")
        #print(x)
        #print(get_terms_list(compressed_dictionary.GetString(),compressed_dictionary.GetTable()))
        ### END TESTING####

        # turn  "sizePL" ,"sizeFreq" ,"location" of each term in __dictionary into a separate list
        posting_lists_size_in_spimi_inverted_index = []
        frequencies_lists_size_in_spimi_inverted_index = []
        location_pointer_to_posting_freq_lists_in_spimi_inverted_index = []

        for ptr in self.__dictionary.values():
            #print(ptr)
            posting_lists_size_in_spimi_inverted_index.append(ptr["sizePL"])
            frequencies_lists_size_in_spimi_inverted_index.append(ptr["sizeFreq"])
            location_pointer_to_posting_freq_lists_in_spimi_inverted_index.append(ptr["location"])

        ## turn all three lists to byte array
        posting_lists_size_in_spimi_inverted_index_byte_array = VBEncode(posting_lists_size_in_spimi_inverted_index)
        frequencies_lists_size_in_spimi_inverted_index_byte_array =  VBEncode(frequencies_lists_size_in_spimi_inverted_index)
        location_pointer_to_posting_freq_lists_in_spimi_inverted_index_to_gaps = calculateGaps(location_pointer_to_posting_freq_lists_in_spimi_inverted_index)
        location_pointer_to_posting_freq_lists_in_spimi_inverted_index_byte_array = VBEncode(location_pointer_to_posting_freq_lists_in_spimi_inverted_index_to_gaps)

        # write posting_lists_size_in_spimi_inverted_index_byte_array in it (on disk)
        write_byte_array_on_disk(posting_lists_size_in_spimi_inverted_index_byte_array , "posting_lists_size_in_inverted_index")
        # write frequencies_lists_size_in_spimi_inverted_index_byte_array in it (on disk)
        write_byte_array_on_disk(frequencies_lists_size_in_spimi_inverted_index_byte_array , "frequencies_lists_size_in_inverted_index")
        # write location_pointer_to_posting_freq_lists_in_spimi_inverted_index_byte_array in it (on disk)
        write_byte_array_on_disk(location_pointer_to_posting_freq_lists_in_spimi_inverted_index_byte_array,"location_pointer_to_posting_freq_lists_in_inverted_index")


        print("=============== INVERTED INDEX FILES ARE ON DISK ===============")



        print("=============== INVERTED INDEX BUILD COMPLETED ===============")





        ###############################



    #####################################
    def SPIMI_Invert(self,file):
        """ Applies the Single-pass in-memory indexing algorithm """
        block_number = 0
        dictionary = {}  # (term - postings list)
        docID = 1
        for doc in file:
            currDoc = ''.join(e for e in doc if e.isalnum())
            if currDoc == '':
                continue  # do not enumerate asterisks, new lines or empty documents

            docAsList = doc.split()  # convert the document a list of strings

            term=''
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
                            tempWord = ''
                    if tempWord != '':
                        term = tempWord.lower()
                ## new Term ##
                # If term occurs for the first time
                if term not in dictionary:
                    dictionary[term] = [docID] # Add term to dictionary, create new postings list, and add docID
                    #if term not in self.__dictionary:
                    #    self.__dictionary[term] = []
                else: # If term has a subsequent occurence
                    dictionary[term].append(docID)  # Add a posting (docID) to the existing posting list of the term



            if sys.getsizeof(dictionary) > self.__block_size_limit :
                #print("\nblock {} size is : {} megabytes ".format( block_number ,sys.getsizeof(dictionary) * 10 ** (-6)))
                self.__blocksDictionaries[block_number] = self.create_block_dictionary(dictionary,block_number) # create block dictionary and write block of posting list and frequencies to disk
                block_number += 1
                dictionary = {}
            docID += 1# enumerate new doc
        if sys.getsizeof(dictionary) > 0 : # if dictionary's size is smaller than block size i.e. last block
            #print("\nblock size is : {} megabytes ".format(block_number,sys.getsizeof(dictionary) * 10 ** (-6)))
            self.__blocksDictionaries[block_number] = self.create_block_dictionary(dictionary,block_number) # create block dictionary and write block of posting list and frequencies to disk

        #self.__dictionary = sorted(self.__dictionary)

    ##################################
    ##################################


    def create_block_dictionary(self,term_postings_list,block_number):
        """

        :param term_postings_list: a dictionary , keys are the terms , value is posting list with duplicates
        :param block_number: number of block
        :return: block dictionary where the key is the term
        and value is a dictionary with keys and values as follows, "sizePL" : posting list size in bytes
                                                                   "sizeFreq" : term frequency list
                                                                   "location" : pointer to the location of the posting list in the block on disk

        """
        block_dictionary = {}
        blockByteArray = bytearray()
        sorted_terms = sorted(term_postings_list)
        pointer_to_postinglist_freqlist = 0
        for term in sorted_terms:
            # print(term)
            result = [int(docIds) for docIds in term_postings_list[term]]  # a list of docIDs that contain the current term
            block_dictionary[term] = self.block_to_byte_array(result)
            block_dictionary[term]["location"] = pointer_to_postinglist_freqlist # location pointer
            pointer_to_postinglist_freqlist = block_dictionary[term]["location"] + block_dictionary[term]["sizePL"] + block_dictionary[term]["sizeFreq"]

            ###############
            # MAYBE KEEP IN THE DICTIONARY AS IS
            # AND POP ENTIRE BLOCK AFTER
            ###############

            blockByteArray+= block_dictionary[term]["pl_freq_byteArray"] ## append current byte array to block array
            block_dictionary[term].pop("pl_freq_byteArray")
            ###############


        #print(block_dictionary)
        #print("\nblock_dictionary {} size is : {} megabytes ".format(block_number, sys.getsizeof(block_dictionary) * 10 ** (-6)))

        ###write block to disk
        self.write_byte_array_block_to_disk(blockByteArray, block_number)

        return block_dictionary

    ###################################
    def block_to_byte_array(self,pl_with_duplicates):
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
        #print(currPostingList)
        #print(frequencyList)
        copmressedCurrPL = PostingList(currPostingList, "V").GetList()
        blockPtrs["pl_freq_byteArray"] = copmressedCurrPL
        blockPtrs["sizePL"]= copmressedCurrPL.__len__()# size of bytearray content in bytes

        ##compressing frequencyList i.e. frequency of each docID
        copmressedFreq = VBEncode(frequencyList)
        blockPtrs["pl_freq_byteArray"] += copmressedFreq
        blockPtrs["sizeFreq"]= copmressedFreq.__len__()# size of bytearray content in bytes

        #print(pl_with_duplicates)
        #print(blockPtrs)
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

    def convert_bytes_block(self,block_number):
        """
        given a block number, converts an entire block of bytes from disk into,posting-frequency list
        and returns a block_dictionary with term as key and ["posting_list .... , frequency_list.... ]  as value
        :param block_number:
        :return: block_dictionary
        """

        block_dictionary = {}
        print("-- Converting bytes from block...")
        #print(block_number)
        path='index_blocks/block-' + str(block_number) + '.txt'

        #print(self.__blocksDictionaries)
        d = self.__blocksDictionaries.pop(block_number) # pointers
        #print(d)
        for item in d.items():
            #print(item[0]) # term
            #print(item[1]) # pointers
            block_dictionary[item[0]] = self.read_bytes_posting_list_from_disk(path, item[1]) ## ["posting_list .... , frequency_list.... ]
            #print(posting_frequency_list)

        #print(block_dictionary)
        return block_dictionary

    #################################################

    def read_bytes_posting_list_from_disk(self,block_path_and_directory,pointers):
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

    #############################################################


    #############################################################
    #############################################################
    #############################################################




    def merge(self , dir):
        """ Merges SPIMI blocks into final inverted index """
        merge_completed = False
        #spimi_index = open(dir + 'spimi_inverted_index.txt', 'a+')

        #for key in self.__blocksDictionaries.keys():
        #   print(key)

        while len(self.__blocksDictionaries.keys()) > 1 :
            tempBlocksDictionary = {}
            lastBlockNum = 0
            for i in range(int(len(self.__blocksDictionaries.keys())/2)):
                tempBlocksDictionary[i] = self.merge_two_blocks(i, max(self.__blocksDictionaries.keys()) ) # merge first and last keys
                lastBlockNum = i

            ######
            #add remaining block to tempBlocksDictionary if number of blocks is odd
            if len(self.__blocksDictionaries.keys())%2 != 0:
                remaining_block = self.__blocksDictionaries.popitem()
                tempBlocksDictionary[lastBlockNum + 1] = remaining_block[1]

            #now self.__blocksDictionaries is empty
            ######
            self.__blocksDictionaries = tempBlocksDictionary
        ####
        # inverted index is block number 0
        ####

    #######
    def merge_two_blocks(self,blkNumA, blkNumB):
        """
        given numbers of two blocks creates a merged block of the two on disk
        and returns its dictionary
        :param blkNumA:
        :param blkNumB:
        :return: block_dictionary like this one {term: {'sizePL': x, 'sizeFreq': y, 'location': z},...,...}
        """
        print("-- merging block {} with block {}...\n".format(blkNumA,blkNumB))
        block_dictionary = {}

        a_block_dictionary = self.convert_bytes_block(blkNumA) #
        #print(a_block_dictionary)
        b_block_dictionary = self.convert_bytes_block(blkNumB) #
        #print(b_block_dictionary)

        while len(a_block_dictionary) != 0 :
            item_a = a_block_dictionary.popitem()
            if len(b_block_dictionary) != 0 :
                item_b = b_block_dictionary.popitem()
                block_dictionary[item_a[0]] = self.merge_term_postings_list(item_a[1],item_b[1])# merge term's posting lists
            else: # no more items in b_block_dictionary
                item_pl = item_a[1][:int(len(item_a[1])/2)]
                item_freq = item_a[1][int(len(item_a[1])/2):]
                block_dictionary[item_a[0]] = self.posting_list_with_duplicates(item_pl,item_freq)

        while len(b_block_dictionary) != 0:# there are more items in b_block_dictionary
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
        block_dictionary_with_pointers = self.create_block_dictionary(block_dictionary,blkNumA)# the merged block will be saved as the block number of blockA
        return block_dictionary_with_pointers


    #############################################################
    #############################################################
    #############################################################

    def merge_term_postings_list(self,a_postings_frequencies,b_postings_frequencies):
        """
         helper method for merge_two_blocks, gets 2 postings_frequencies list of a term
         and returns a merged posting_list_with_duplicates list
        :param a_postings_frequencies: posting list and frequencies list of block a
        :param b_postings_frequencies: posting list and frequencies list of block b
        :return: posting_list_with_duplicates
        """
        #####
        a_pl = a_postings_frequencies[:int(len(a_postings_frequencies)/2)]
        a_freq = a_postings_frequencies[int(len(a_postings_frequencies)/2):]
        a_posting_list_with_duplicates =self.posting_list_with_duplicates(a_pl,a_freq)
        #####
        b_pl = b_postings_frequencies[:int(len(b_postings_frequencies) / 2)]
        b_freq = b_postings_frequencies[int(len(b_postings_frequencies) / 2):]
        b_posting_list_with_duplicates =self.posting_list_with_duplicates(b_pl,b_freq)
        ######
        return sorted(a_posting_list_with_duplicates + b_posting_list_with_duplicates)

    #########################
    def posting_list_with_duplicates(self,postings_list,frequencies_list):
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






    #############################################################

    def delete_block(self,block_number):
        """
        given a block number removes block from disk
        :param block_number:
        :return:
        """
        print("-- deleting block number...", block_number)
        path = 'index_blocks/block-' + str(block_number) + '.txt'
        os.remove(path)

    #############################################################




    def merge_blocks(self , blocks,dir):
        """ Merges SPIMI blocks into final inverted index """
        merge_completed = False
        spimi_index = open(dir + 'spimi_inverted_index.txt', 'a+')
        # Collect initial pointers to (term : postings list) entries of each SPIMI blocks
        temp_index = OrderedDict()
        for num, block in enumerate(blocks):
            print("-- Reading into memory...", blocks[num].name)
            line = blocks[num].readline()  # term:[docID1, docID2, docID3]
            line_tpl = line.rsplit(':', 1)
            term = line_tpl[0]
            postings_list = ast.literal_eval(line_tpl[1])
            temp_index[num] = {term: postings_list}
            print(temp_index)
        while not merge_completed:
            # Convert into an array of [{term: [postings list]}, blockID]
            tpl_block = ([[temp_index[i], i] for i in temp_index])
            # Fetch the current term postings list with the smallest alphabetical term
            smallest_tpl = min(tpl_block, key=lambda t: list(t[0].keys()))
            # Extract term
            smallest_tpl_term = (list(smallest_tpl[0].keys())[0])
            # Fetch all IDs of blocks that contain the same term in their currently pointed (term: postings list) :
            # For each block, check if the smallest term is in the array of terms from all blocks then extract the block id
            smallest_tpl_block_ids = [block_id for block_id in temp_index if
                                      smallest_tpl_term in [term for term in temp_index[block_id]]]
            # Build a new postings list which contains all postings related to the current smallest term
            # Flatten the array of postings and sort
            smallest_tpl_pl = sorted(
                sum([pl[smallest_tpl_term] for pl in (temp_index[block_id] for block_id in smallest_tpl_block_ids)],
                    []))
            spimi_index.write(str(smallest_tpl_term) + ":" + str(smallest_tpl_pl) + "\n")

            # Collect the next sectioned (term : postings list) entries from blocks that contained the previous smallest tpl term
            for block_id in smallest_tpl_block_ids:
                # Read the blocks and read tpl in a temporary index
                block = [file for file in blocks if re.search('block-' + str(block_id), file.name)]
                if block[0]:
                    line = block[0].readline()
                    if not line == '':
                        line_tpl = line.rsplit(':', 1)
                        term = line_tpl[0]
                        postings_list = ast.literal_eval(line_tpl[1])
                        temp_index[block_id] = {term: postings_list}
                    else:
                        # Delete block entry from the temporary sectioned index holder if no line found
                        del temp_index[block_id]
                        blocks.remove(block[0])
                        print("Finished merging block:", block[0].name)
                else:
                    blocks.remove(block[0])
            # If all block IO streams have been merged
            if not blocks:
                merge_completed = True
                print("SPIMI completed! All blocks merged into final index: spimi_inverted_index.txt")
        return spimi_index.name





    def removeIndex(self, dir):
        """Delete all index files by removing the given directory
        dir is the name of the directory in which all index files are located. After removing the files, the directory should be deleted."""
        self.deleteblocks()
        os.rmdir(dir)

    def deleteblocks(self):
        """ Delete directories and files under ./index_blocks"""
        for root, directories, files in os.walk('./index_blocks'):
            for f in files:
                os.unlink(os.path.join(root, f))
            for directory in directories:
                shutil.rmtree(os.path.join(root, directory))



######################
def write_byte_array_on_disk(byte_array, file_name):
    """
    given a byte array and a file name, creates a file named file_name and writes byte array in it
    :param byte_array:
    :param file_name:
    :return:
    """
    # Define block
    path = 'index_blocks/'
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
    return array



#####################
###################        TESTING               #########################


#alanspath="C:/Users/alany/OneDrive/Desktop/files/1000.txt"
mypath = '/Users/yairwygoda/Desktop/1000000.txt'
indexW = IndexWriter(mypath ,'index_blocks/')
#print("deleting index files and directory")
#indexW.removeIndex('index_blocks/')





'''
with open("index_blocks/block-0.txt", "rb") as binary_file:
    # Seek a specific position in the file and read N bytes
    binary_file.seek(0, 0)  # Go to beginning of the file
    couple_bytes = binary_file.read(2)
    print(couple_bytes)
'''



