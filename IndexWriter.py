class IndexWriter:
    __dicMap={}

    def __init__(self, inputFile, dir):
        f = open(inputFile, "r")
        docsList =  self.createDocsList(f)
        numOfWords= 0
        for doc in docsList:
            numOfWords+=len(doc[1])
        print("number of words : {}".format(numOfWords))

        for item in self.__dicMap.items():
            # print(item)
            pass

        ### store __dicMap on disk as byte file   ##

        """Given a collection of documents, creates an on disk index
        inputFile is the path to the file containing the review data (the path includes the filename itself)
        dir is the name of the directory in which all index files will be created
        if the directory does not exist, it should be created"""

    #Enumarates docs of file in an ascending order
    def createDocsList(self,file):
        '''
        Enumarates docs of the inputFile in an ascending order
        for each doc : 1.Get rid of non - alphanumeric characters
                       2.Normalize text to lower-case
                       3.Sort words in the Doc in an ascending order
        :param file: a string of documents separated by 80 asterisks
        :return: a list of tuples, the first argument is the docID and the second is the actual text as a list of strings
        '''
        docsList = []
        docID = 1
        termID = 1
        termIDtoDocIDlist=list()

        for doc in file:
            currDoc = ''.join(e for e in doc if e.isalnum())
            if currDoc == '':
                continue # do not enumerate asterisks, new lines or empty documents
            docAsList = doc.split() # convert the document a list of strings
            tokensList = []
            # enumerate tokens for the __dicMap
            for word in docAsList:
                # a method : term to termID
                #           create a dictionary to map (hash map) all words in the input file (unrelated to actual dictionary to compress)
                #           store this map on on disk as byte file ( to open it specify that it's a binary file )
                #

                # add to list only alphanumeric tokens
                if word.isalnum(): # word is alphanumeric
                    tokensList.append(word.lower())
                    if word.lower() not in self.__dicMap.keys(): # first appearance of the term
                        self.__dicMap[word.lower()]= termID # add term to dictionary
                        termIDtoDocIDlist.append((termID, docID)) # add tuple (termID,docID) to termID to DocID list
                        termID += 1
                    else: # term is already in dicMap
                        termIDtoDocIDlist.append((self.__dicMap[word.lower()], docID)) # add tuple (termID,docID) to termID to DocID list


                else: # word is NOT alphanumeric
                    tempWord = ''
                    for char in word:
                        if char.isalnum():
                            tempWord+=char
                        elif tempWord != '':
                            tokensList.append(tempWord.lower())
                            if tempWord.lower() not in self.__dicMap.keys(): # first appearance of the term
                                self.__dicMap[tempWord.lower()] = termID # add term to dictionary
                                termIDtoDocIDlist.append((termID, docID)) # add tuple (termID,docID) to termID to DocID list
                                termID += 1
                            else:  # term is already in dicMap
                                termIDtoDocIDlist.append((self.__dicMap[tempWord.lower()], docID))  # add tuple (termID,docID) to termID to DocID list
                            tempWord = ''
                    if tempWord != '':
                        tokensList.append(tempWord.lower())
                        if tempWord.lower() not in self.__dicMap.keys(): # first appearance of the term
                            self.__dicMap[tempWord.lower()] = termID # add term to dictionary
                            termIDtoDocIDlist.append((self.__dicMap[tempWord.lower()], docID))# add tuple (termID,docID) to termID to DocID list
                            termID += 1
                        else: # term is already in dicMap
                            termIDtoDocIDlist.append((self.__dicMap[tempWord.lower()], docID))  # add tuple (termID,docID) to termID to DocID list
            tokensList.sort()
            docsList.append((docID,tokensList))
            docID+=1
        self.__dicMap = dict(sorted(self.__dicMap.items())) # sort dictionary

        ########    WRITE DICTIONARY ON DISK ##########
        ########    WRITE DICTIONARY ON DISK ##########
        ########    WRITE DICTIONARY ON DISK ##########

        print("tokens number is: {}".format(termID))
        for tup in termIDtoDocIDlist:
            #print(tup)
            pass

        return docsList





    def removeIndex(self, dir):
        pass
        """Delete all index files by removing the given directory
        dir is the name of the directory in which all index files are located. After removing the files, the directory should be deleted."""


indexW = IndexWriter('/Users/yairwygoda/Desktop/100.txt','newdir')