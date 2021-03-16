import Server.misc as misc

class Table:
    def __init__(self):
        self.wholeTable = [[() for i in range(48)] for j in range(7)]
        self.bookingList = []
        self.callbackList = [] #Facility name, interval time, ip add, port

    def getTable(self):
        print(self.wholeTable)
        return



    def updateTable(self, j, i, value):
        if self.wholeTable[j][i] == ():
            self.wholeTable[j][i] = (value,)
        else:
            for k in range(len(self.wholeTable[j][i])):
                if (value in self.wholeTable[j][i]):
                    raise Exception("Conflict booking "+ value + " on " + misc.Misc.slot2time(i))
            self.wholeTable[j][i] = (self.wholeTable[j][i] + (value,)) 
        

    def updateTableRemove(self, j, i, value):
        
        for k in range(len(self.wholeTable[j][i])):
            self.wholeTable[j][i] = list(self.wholeTable[j][i])
            if (value == (self.wholeTable[j][i])[k]):
                # print("k", k)
                # print("value", value)
                # print(self.wholeTable[j][i])
                #self.wholeTable[j][i] = list(self.wholeTable[j][i])
                #print(self.wholeTable[j][i])
                self.wholeTable[j][i].pop(k)
                #print("poped:", self.wholeTable[j][i])
                self.wholeTable[j][i] = tuple(self.wholeTable[j][i])
                
            
        #if x==0:
        #       raise Exception("Invalid")
        #self.wholeTable[j][i] = (self.wholeTable[j][i] + (value,))     

    def searchTable(self, nameStr, j):
        #for j in range(7) >>j is day range
        searched = list(self.wholeTable[j][:])
        timeSlot = []
        for i in range(len(searched)):
            if (nameStr in searched[i]) == False:
                strTime = misc.Misc.slot2time(i)
                #print(strTime) #can use this to see when is it free
                timeSlot.append(strTime)
        return timeSlot

    def booking(self, nameSlot, daySlot, startSlot, endSlot):
        # check if available
        for i in range(startSlot, endSlot):
            if self.wholeTable[daySlot][i] == 1: #name not settled
                raise Exception("Conflict booking")

        for i in range(startSlot, endSlot):
            self.wholeTable[daySlot][i] = 1 #name not settled

        bookingID = BookingList.setBookingList(nameSlot, daySlot, startSlot, endSlot)

        return bookingID

