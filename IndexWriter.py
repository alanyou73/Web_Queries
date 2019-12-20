class IndexWriter:
    def __init__(self, inputFile, dir):
        f = open(inputFile, "r")
        docsList =  self.createDocsList(f)
        for doc in docsList:
            print("document ID : {} \n{}".format(doc[0],doc[1]))



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
        docNum = 1
        for doc in file:
            currDoc = ''.join(e for e in doc if e.isalnum())
            if currDoc == '':
                continue # do not enumerate asterisks, new lines and empty documents
            #print("doc number : {}".format(docNum))
            docAsList = doc.split() # convert the document a list of strings
            tokensList = []
            # add to list only alphanumeric tokens
            for word in docAsList:
                # a method : term to termID
                #           create a dictionary to map (hash map) all words in the input file (unrealted to actual dictionary to compress)
                #           store this map on on disk as byte file ( to open it specify that it's a binary file )
                #
                if word.isalnum():
                    tokensList.append(word.lower())
                else:
                    tempWord = ''
                    for char in word:
                        if char.isalnum():
                            tempWord+=char
                        elif tempWord != '':
                            tokensList.append(tempWord.lower())
                            tempWord = ''
                    if tempWord != '':
                        tokensList.append(tempWord.lower())
            tokensList.sort()
            docsList.append((docNum,tokensList))
            docNum+=1
        return docsList





    def removeIndex(self, dir):
        pass
    """Delete all index files by removing the given directory
    dir is the name of the directory in which all index files are located. After removing the files, the directory should be deleted."""


indexW = IndexWriter('/Users/yairwygoda/Desktop/100.txt','newdir')