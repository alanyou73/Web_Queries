import os
import sys
import collections
import ast
import re
import shutil

from os import listdir
from collections import OrderedDict

class IndexWriter:
    #__block_size_limit = 2147483648 # 2GB
    #__block_size_limit = 1073741824 # 1GB
    #__block_size_limit = 750000  # default block size (in bytes) 0.75 MB
    __block_size_limit = 100000  # default block size (in bytes) 0.75 MB
    __dir =""
    def __init__(self, inputFile, dir):

        """Given a collection of documents, creates an on disk index
        inputFile is the path to the file containing the review data (the path includes the filename itself)
        dir is the name of the directory in which all index files will be created
        if the directory does not exist, it should be created"""
        self.__dir = dir
        if not os.path.exists(dir):
            os.mkdir(dir)
        file = open(inputFile, "r")
        if os.path.exists(dir + 'spimi_inverted_index.txt'):
            os.remove(dir + 'spimi_inverted_index.txt' )
        self.SPIMI_Invert(file)

        print("=============== Merging SPIMI blocks into final inverted index... ===============")

        spimi_blocks = [open(dir + block) for block in listdir(dir) if block !=".DS_Store" ] # do not count ".DS_Store" file as a block for macOS
        merged_spimi_blocks = self.merge_blocks(spimi_blocks,dir) # merges blocks into one file and returns it's pathname
        #print(merged_spimi_blocks)
        #remove blocks from disk
        for file in listdir(dir):
            if file == merged_spimi_blocks[len(dir):] :
                continue
            os.remove(dir + file)



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
                else: # If term has a subsequent occurence
                    dictionary[term].append(docID)  # Add a posting (docID) to the existing posting list of the term

            if sys.getsizeof(dictionary) > self.__block_size_limit :
                temp_dict = self.sort_terms(dictionary)
                self.write_block_to_disk(temp_dict, block_number)
                temp_dict = {}
                block_number += 1
                dictionary = {}
            # enumerate new doc
            docID += 1
        # if dictionary's size is smaller than block size i.e. last block
        if sys.getsizeof(dictionary) > 0 :
            temp_dict = self.sort_terms(dictionary)
            self.write_block_to_disk(temp_dict, block_number)
            temp_dict = {}

    def sort_terms(self,term_postings_list):
        """ Sorts dictionary terms in alphabetical order """
        print(" -- Sorting terms...")
        sorted_dictionary = OrderedDict()  # keep track of insertion order
        sorted_terms = sorted(term_postings_list)
        for term in sorted_terms:
            result = [int(docIds) for docIds in term_postings_list[term]]
            result_tftd = self.calculate_tftd(result)
            sorted_dictionary[term] = result_tftd
        return sorted_dictionary

    def calculate_tftd(self,pl_with_duplicates):
        """ Add term frequency of term in each document """
        # print(pl_with_duplicates)
        counter = collections.Counter(pl_with_duplicates)
        pl_tftd = [[int(docId), counter[docId]] for docId in counter.keys()]
        return pl_tftd

    def write_block_to_disk(self,term_postings_list, block_number):
        """ Writes index of the block (dictionary + postings list) to disk """
        # Define block
        base_path = 'index_blocks/'
        block_name = 'block-' + str(block_number) + '.txt'
        block = open(base_path + block_name, 'a+')
        print(" -- Writing term-positing list block: " + block_name + "...")
        # Write term : posting lists to block
        for index, term in enumerate(term_postings_list):
            # Term - Posting List Format
            # term:[docID1, docID2, docID3]
            # e.g. cat:[4,9,21,42]
            block.write(str(term) + ":" + str((term_postings_list[term])) + "\n")
        block.close()

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



indexW = IndexWriter('/Users/yairwygoda/Desktop/100.txt','index_blocks/')
#print("deleting index files and directory")
#indexW.removeIndex('index_blocks/')