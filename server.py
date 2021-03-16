import asyncio
import Server.table as table
import Server.misc as misc

from serialization.derived import String, create_union_type, create_array_type, create_struct_type
from serialization.numeric import i64, u8
Arrayu8 = create_array_type('u8', u8)

def __init__(self, table:table.Table):
    self.thisTable = table()
    

async def server():
    print("hello")
    wholeTable = table.Table()
    #print(wholeTable.availTable)

async def avail(nameStr: String, day: String, thisTable:table.Table) -> Arrayu8:
    try:
        #nameNum = Misc.name2Num(nameStr)
        dayNum = misc.Misc.day2Num(day)
        print(dayNum)
        #thisTable.getTable()
        #thisTable.updateTable(dayNum, 4, nameStr)
        #strTime = misc.Misc.slot2time(31)
        #print(strTime)
        #thisTable.updateTable(dayNum, 4, "Tutorial Room")
        #print(table.Table.getTable())
        searched = thisTable.searchTable(nameStr, dayNum)
        #client can do this to print out the available slot
        # for i in range(len(searched)):
        #     if searched[i] == ():
        #         strTime = misc.Misc.slot2time(i)
        #         print(strTime)
        return searched
    except ValueError as e:
        print("Error at input", e)

async def booking(nameStr: String, day: String, startHour: u8, startMin: u8, endHour: u8, endMin: u8, thisTable: table.Table) -> u8:
    try:
        startSlot = misc.Misc.time2slot(startHour,startMin)
        endSlot = misc.Misc.time2slot(endHour,endMin)
        dayNum = misc.Misc.day2Num(day)
        
        for i in range(startSlot,endSlot):
            thisTable.updateTable(dayNum,i,nameStr)

        searched = thisTable.searchTable(nameStr, dayNum)
        nameNum = misc.Misc.name2Num(str(nameStr))
        confirmID = int(str(nameNum)+str(dayNum)+str(startSlot)+str(endSlot))
        return confirmID
    except ValueError as e:
        print("Error at input", e)

async def main():
    await server()
    thisTable = table.Table()
    freeTable = await avail("Meeting Room", "Monday", thisTable)
    print("Free Table:", freeTable)
    book = await booking("Reading Room", "Monday", 1,30,19,00,thisTable)
    book = await booking("Study Room", "Monday", 1,30,10,00,thisTable)
    #book = await booking("Study Room", "Monday", 0,00,0,30,thisTable)
    print("Booking ID:", book)
    #freeTable = await avail("Meeting Room", "Monday", thisTable)
    #print("Free Table:", freeTable)


asyncio.run(main())
