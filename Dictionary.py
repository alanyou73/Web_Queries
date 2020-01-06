class Dictionary:

    __dictionaryStr = ""
    __table = []
    type = ""
    k = 0
    def __init__(self, TermList, type):
        """Given sorted list of terms, creates a data
        structure which holds a compressed dictionary
        TermList is the sorted list of terms
        Type is one of the following tupples: ("STR"),
        ("BLK", k), ("FC", k) where k is the size of
        the block"""
        if type == "STR":
            self.TermList = TermList
            self.type = type
            counter = 0
            for term in TermList:
                self.__dictionaryStr += term
                self.__table.append(counter)
                counter+=len(term)
            #print(self.__table)
        else:
            self.__table = []
            self.type ,self.k = type
            add = len(TermList)%self.k
            self.TermList = TermList
            for i in range(add):
                self.TermList.append(None)
            #print(self.TermList)

            if self.type == "BLK":
                for i in range(0,len(TermList),self.k):
                    if self.TermList[i] == None:
                        break
                    block = [self.termLocation(self.TermList[i])]
                    count = 0
                    while count < self.k:
                        if self.TermList[i+count] == None:
                            break
                        self.__dictionaryStr += self.TermList[i+count]
                        block.append(len(self.TermList[i+count]))
                        count+=1
                    self.__table.append(tuple(block))
            elif self.type == "FC":
                track = 0
                for i in range(0, len(TermList), self.k):
                    if self.TermList[i] == None:
                        break
                    block = [track,(len(self.TermList[i]),0)]
                    track+=len(self.TermList[i])
                    count = 1
                    self.__dictionaryStr += self.TermList[i]
                    tracking = 0
                    while count < self.k  :

                        currWord = self.TermList[i+count - 1]
                        nextWord = self.TermList[i+count]
                        if currWord == None:
                            break
                        prefix = self.calcPrefix(currWord, nextWord)# CALCULATE PREFIX
                        tracking +=len(nextWord)-len(prefix)
                        self.__dictionaryStr += nextWord[len(prefix):]
                        block.append((len(nextWord), len(prefix)))
                        count += 1
                    track +=tracking
                    self.__table.append(tuple(block))



    #########################################
    def calcPrefix(self, curr, next):
        prefix = ""
        for i in range(len(curr)):
            if i < len(next):
                if curr[i] == next[i]:
                    prefix += curr[i]
                else:
                    return prefix
        return prefix
    #########################################

    def GetString(self):
        """Returns the dictionary's string"""
        return self.__dictionaryStr
    def GetInfo(self, term):
        """ Returns relevant data about term.
        For "STR" it returns the location of the term
        in the string
        For "BLK" it returns a tuple containing the
        location of the container block and the lengths
        of its terms
        For "FC" it returns a tuple containing the
        location of the container block and pairs
        containing the lengths and prefixes sizes of
        its terms"""
        location = self.termLocation(term)
        if self.type == "STR":
            return location
        elif self.type == "BLK":
            for tup in self.__table:
                index =1
                currLocation = tup[0]
                while index <= self.k:
                    if location == currLocation :
                        return tup
                    currLocation += tup[index]#pointer to next term in the block
                    index += 1
        elif self.type == "FC":
            index = 0
            for word in self.TermList:
                if term == word:
                    return self.__table[(index%self.k)]
                index+=1


    #############################

    def termLocation(self,term):
        location = 0
        for word in self.TermList:
            if word == term:
                return location
            location += len(word)
        return None

     ############################

    def GetTable(self):
        return self.__table



def get_terms_list(dictionary_str , table):
    """
    given a compressed dictinary STR and its table , returns its terms as a list
    :param dictionary_str:
    :param table:
    :return: dict - dictionary as list
    """
    dict = []
    for i,loc in enumerate(table):
        if i == len(table)-1:
            dict.append(dictionary_str[loc:])
            break
        dict.append(dictionary_str[loc:table[i+1]])
    return dict


################################          TESTER               #########################################################
'''


strArray=["ba", "banana", "car", "cat", "dog", "doggy", "dump", "far", "formula", "in", "input", "int"]

print("########    TESTING STR        ##############")
dic1 = Dictionary(strArray,"STR")
print(dic1.GetString())
print(dic1.GetInfo("banana"))


print("########   FINISHED TESTING     ##############")
print("########    TESTING STR         ##############")
print("BLOCK SIZE : 2 ")
dic2 = Dictionary(strArray,("BLK",2))
print("BLOCK SIZE : 3 ")
dic3 = Dictionary(strArray,("BLK",3))
print("BLOCK SIZE : 4 ")
dic4 = Dictionary(strArray,("BLK",4))
print(dic4.GetInfo("car"))
print(dic4.GetInfo("dog"))
print(dic4.GetInfo("int"))
print("BLOCK SIZE : 5")
dic5= Dictionary(strArray,("BLK",5))
print(dic5.GetInfo("car"))
print(dic5.GetInfo("doggy"))
print(dic5.GetInfo("int"))
print("BLOCK SIZE : 6 ")
dic6 = Dictionary(strArray,("BLK",6))
print(dic6.GetInfo("car"))
print(dic6.GetInfo("dump"))
print(dic6.GetInfo("int"))
print("BLOCK SIZE : 7 ")
dic7 = Dictionary(strArray,("BLK",7))
print(dic7.GetInfo("car"))
print(dic7.GetInfo("far"))
print(dic7.GetInfo("int"))
print("BLOCK SIZE : 8 ")
dic8 = Dictionary(strArray,("BLK",8))
print(dic8.GetInfo("car"))
print(dic8.GetInfo("formula"))
print(dic8.GetInfo("int"))

print("########   FINISHED TESTING     ##############")
print("########    TESTING FC          ##############")
print("FRONT CODING , BLOCK SIZE : 2 ")
dicFC2 = Dictionary(strArray,("FC",2))
print(dicFC2.GetInfo("car"))
print(dicFC2.GetInfo("dog"))
print(dicFC2.GetInfo("int"))

print("FRONT CODING , BLOCK SIZE : 3 ")
dicFC3 = Dictionary(strArray,("FC",3))
print(dicFC3.GetInfo("car"))
print(dicFC3.GetInfo("dog"))
print(dicFC3.GetInfo("int"))
print("FRONT CODING , BLOCK SIZE : 4 ")
dicFC4 = Dictionary(strArray,("FC",4))
print(dicFC4.GetString() == "bananacartdoggyumpfarformulainputt")
print(dicFC4.GetInfo("car"))
print(dicFC4.GetInfo("dog"))
print(dicFC4.GetInfo("int"))



print("########   FINISHED TESTING     ############")



'''

