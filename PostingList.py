class PostingList:
    type = ""
    __gaps =[]
    __postingList = []
    def __init__(self, DocIDs, type):
        '''
        Given sorted list of DocIDs, creates a
        compressed posting list according to the
        compression type.
        The compressed posting list should be stored as
        byte array
        '''

        self.type = type
        self.DocIDs = DocIDs
        #calculateGaps
        self.__gaps = self.calculateGaps(DocIDs)
        #print(self.__gaps)
        if type == "V": # Varint
                self.__postingList = self.VBEncode(self.__gaps)
        elif type == "LP": # Length precoded
            self.__postingList = self.LPEncode(self.__gaps)
        elif type == "GV": # Group varint
            self.__postingList = self.GVEncoding(self.__gaps)
    ###############
    def calculateGaps(self,IDs):
        '''
        :param IDs: a list of posting IDs
        :return: the array of gaps between the IDs
        '''
        gaps = [IDs[0]]
        index = 1
        while index < len(IDs):
            gaps.append(IDs[index]-IDs[index-1])
            index+=1
        return gaps

    ###############
    def VBEncodeNumber(self,n):
        bytesList = bytearray()
        bitsArray = []
        binaryNum = bin(n).replace("0b", "")
        #
        curr = binaryNum[-7:]
        curr = curr[::-1]
        zeros = '0' * (7 - len(curr))  # CALCULATE ZEROS
        curr += zeros  # ADD ZEROS
        curr += '1'
        curr = curr[::-1]
        binaryNum = binaryNum[: - 7]
        if len(binaryNum) == 0:
            bytesList.append(int(curr, 2))
            return bytesList
        bitsArray.append(curr)

        while True:
            if len(binaryNum) <= 7:
                zeros = '0' * (8 - len(binaryNum))  # CALCULATE ZEROS
                binaryNum = binaryNum[::-1]  # INVERT
                binaryNum += zeros  # ADD ZEROS
                binaryNum = binaryNum[::-1]  # INVERT

                bitsArray.append(binaryNum)  # ADD TO ARRAY
                break
            curr = binaryNum[-7:]
            curr = curr[::-1]
            curr += '0'
            curr = curr[::-1]
            bitsArray.append(curr)
            binaryNum = binaryNum[: - 7]
        bitsArray.reverse()

        for byte in bitsArray:
            bytesList.append(int(byte, 2))
        return bytesList
    ###############

    def VBEncode(self,numbers):
        byteStream = bytearray()
        for number in numbers:
            if self.type =="V":
                byteStream += self.VBEncodeNumber(number)
        return byteStream
    ###############
    def LPEncode(self,numbers):
        byteStream = bytearray()
        for number in numbers:
            byteStream += self.LPEncodingNumber(number)
        return byteStream
    ###############
    def LPEncodingNumber(self,n):
        bytesList = bytearray()
        binaryNum = bin(n).replace("0b", "")
        byteLength = ''
        if len(binaryNum) < 7:
            byteLength = '00'  # 1
            zeros = '0' * (6 - len(binaryNum))  # CALCULATE ZEROS
            byteLength += zeros
            byteLength += binaryNum
            bytesList.append(int(byteLength, 2))
            return bytesList
        elif len(binaryNum) < 15:
            byteLength = '01'  # 2
            zeros = '0' * (14 - len(binaryNum))  # CALCULATE ZEROS
            byteLength += zeros
            byteLength += binaryNum
            bytesList = self.bitsToBytes(byteLength)
            return bytesList
        elif len(binaryNum) < 23:
            byteLength = '10'  # 3
            zeros = '0' * (22 - len(binaryNum))  # CALCULATE ZEROS
            byteLength += zeros
            byteLength += binaryNum
            bytesList = self.bitsToBytes(byteLength)
            return bytesList
        elif len(binaryNum) < 31:
            byteLength = '11'  # 4
            zeros = '0' * (30 - len(binaryNum))  # CALCULATE ZEROS
            byteLength += zeros
            byteLength += zeros
            byteLength += binaryNum
            bytesList = self.bitsToBytes(byteLength)
            return bytesList
        else:
            print("Number is too long, can't encode")
            return None
    ###############

    def bitsToBytes(self,bits):
        bytes = bytearray()
        count = len(bits) / 8
        temp = bits
        currByte = ''
        while count > 0:
            currByte = temp[:8]
            bytes.append(int(currByte, 2))
            temp = temp[8:]
            count -= 1
        return bytes

    ###############
    def GVEncoding(self,numbers):
        bytesList = bytearray()
        tempNumbersList = numbers
        while len(tempNumbersList) % 4 != 0:
            tempNumbersList.append(0)  # append zeros to numbers list till the length of list is divisible by 4
        while len(tempNumbersList) > 0:
            if self.GVEncodingChunk(tempNumbersList[:4]) == None:
                print("One of the numbers wasn't valid , couldn't encode")
                return None
            bytesList += self.GVEncodingChunk(tempNumbersList[:4])  # .append()
            tempNumbersList = tempNumbersList[4:]
        return bytesList

    ###############
    def GVEncodingChunk(self,numbers):
        '''
        :param numbers: list of 0 to 4 numbers
        :return: a byte array of the Group Varint of the given list
        '''
        finalBytesList = bytearray()
        currBytesList = bytearray()
        if len(numbers) > 4:
            print("To many numbers, can't encode")
            return None
        mask = ''
        for num in numbers:
            binaryNum = bin(num).replace("0b", "")
            byteNum = ''
            if len(binaryNum) > 32:
                print("Number is too long, can't encode")
                return None
            elif len(binaryNum) < 9:
                mask += '00'
                zeros = '0' * (8 - len(binaryNum))  # CALCULATE ZEROS
                byteNum += zeros
                byteNum += binaryNum
                currBytesList.append(int(byteNum, 2))
            elif len(binaryNum) < 17:
                mask += '01'
                zeros = '0' * (16 - len(binaryNum))  # CALCULATE ZEROS
                byteNum += zeros
                byteNum += binaryNum
                ####Bits to Bytes
                currBytesList += self.bitsToBytes(byteNum)
            elif len(binaryNum) < 25:
                mask += '10'
                zeros = '0' * (24 - len(binaryNum))  # CALCULATE ZEROS
                byteNum += zeros
                byteNum += binaryNum
                ####Bits to Bytes
                currBytesList += self.bitsToBytes(byteNum)
            elif len(binaryNum) < 33:
                mask += '11'
                zeros = '0' * (32 - len(binaryNum))  # CALCULATE ZEROS
                byteNum += zeros
                byteNum += binaryNum
                ####Bits to Bytes
                currBytesList += self.bitsToBytes(byteNum)
        finalBytesList.append(int(mask, 2))  # append mask to finalBytesList
        finalBytesList += currBytesList  # appned currBytesList to finalBytesList
        return finalBytesList



    ########################
    def VBDecode(self):
        numbers = []
        n = 0
        for i in self.__postingList:
            if i < 128 :
                n = 128*n + i
            else:
                n = 128*n + (i - 128)
                numbers.append(n)
                n = 0

        #call unGap
        return numbers
    ###############
    def GetList(self):
        '''
        Returns a byte-array containing the
        compressed string
        '''
        return self.__postingList

    def GetGaps(self):
        return self.__gaps

    def unGap(self , gaps):
        """
        :param gaps:
        :return: the original posting list
        """
        originalList = []
        temp = 0
        for num in gaps:
            temp += num
            originalList.append(temp)
        return originalList






###########################################################

def VBEncode(numbers):
    byteStream = bytearray()
    for number in numbers:
        byteStream += VBEncodeNumber(number)
    return byteStream




def VBEncodeNumber(n):
    bytesList = bytearray()
    bitsArray = []
    binaryNum = bin(n).replace("0b", "")
    #
    curr = binaryNum[-7:]
    curr = curr[::-1]
    zeros = '0' * (7 - len(curr))  # CALCULATE ZEROS
    curr += zeros  # ADD ZEROS
    curr += '1'
    curr = curr[::-1]
    binaryNum = binaryNum[: - 7]
    if len(binaryNum) == 0:
        bytesList.append(int(curr, 2))
        return bytesList
    bitsArray.append(curr)

    while True:
        if len(binaryNum) <= 7:
            zeros = '0' * (8 - len(binaryNum))  # CALCULATE ZEROS
            binaryNum = binaryNum[::-1]  # INVERT
            binaryNum += zeros  # ADD ZEROS
            binaryNum = binaryNum[::-1]  # INVERT

            bitsArray.append(binaryNum)  # ADD TO ARRAY
            break
        curr = binaryNum[-7:]
        curr = curr[::-1]
        curr += '0'
        curr = curr[::-1]
        bitsArray.append(curr)
        binaryNum = binaryNum[: - 7]
    bitsArray.reverse()

    for byte in bitsArray:
        bytesList.append(int(byte, 2))
    return bytesList


##################################################################




###############################
def toBytearray(line):
    """
    :param line: line of bytes from file
    :return: bytearray()
    """
    b = bytearray()
    for x in line:
        b.append(x)
    return b

############################

def VBDecode(postingList):
    numbers = []
    n = 0
    for i in postingList:
        if i < 128 :
            n = 128*n + i
        else:
            n = 128*n + (i - 128)
            numbers.append(n)
            n = 0

    #call unGap
    return numbers
###############################
def unGap(gaps):
    """
    :param gaps:
    :return: the original posting list
    """
    originalList = []
    temp = 0
    for num in gaps:
        temp += num
        originalList.append(temp)
    return originalList

##############################

def calculateGaps(array):
    """

    :param array: list of numbers
    :return: a list of the gaps between the array values
    """
    gaps = [array[0]]
    index = 1
    while index < len(array):
        gaps.append(array[index]-array[index-1])
        index+=1
    return gaps

###################################################

















'''

#####################################             TESTER          ######################################################

testList = [7,12,23,1033,2354634,2354636]
###initializing big numbers for testing purposes##
biggerThen32Bits = int('100000000000000000000000000000000',2)# decimal ===> 4294967296
smallerThan23Bits = int('11111111111111111111111111111111',2) # decimal ===> 4294967295

###########################################################################
###########################################################################

print('#####       TESTING  V       ########')
postListV = PostingList(testList,"V")
print(postListV.GetList())
print("".join("\\x%02x" % i for i in postListV.GetList()))
print('\n')
###########################################################################
###########################################################################

print('#####       TESTING  LP    ########')
postListLP = PostingList(testList,"LP")
print(postListLP.GetList())
print('\n')
###########################################################################
###########################################################################

print('#####      TESTING  GV      ########')

bigNumberTestListGV = [44, 63, 256, 2354634,smallerThan23Bits]

postListGV = PostingList(testList,"GV") # testList
postListGV1 = PostingList(bigNumberTestListGV,"GV") # bigNumberTestListGV
print(postListGV.GetList()) # output ====>  bytearray(b'\x01\x07\x05\x0b\x03\xf2\x80#\xe9\xc1\x02\x00\x00')
print(postListGV1.GetList()) # output ====> bytearray(b'\x02,\x13\xc1#\xec\xca\xc0\xff\xdc\x125\x00\x00\x00')

#####################################     END   OF TESTER     ##########################################################



'''

############################################################################
'''
    #############PSEUDO CODE############
    def VBEncodeNumber(self,n):
        bytesList = bytearray() #initialize byte array
        temp = n
        count = 0
        while True:
            bytesList.append(temp%128)
            if temp < 128:
                break
            temp = temp/128
            count +=1
        bytesList[count]+=128 # add the c bit to the last element of the byte array
        return bytesList

'''