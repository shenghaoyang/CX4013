class Misc:
    #static method wont be expecting the first arg to be a instance
    def name2Num(name): 
        try:
            return {
                "Meeting Room": 1,
                "Lecture Room": 2,
                "Tutorial Room": 3,
                "Reading Room": 4,
                "Study Room": 5
            }[name]
        except KeyError:
            raise ValueError[name]

    def day2Num(day):
        try:
            return {
                "Monday": 0,
                "Tuesday": 1,
                "Wednesday": 2,
                "Thursday": 3,
                "Friday": 4,
                "Saturday": 5,
                "Sunday": 6
            }[day]
        except KeyError:
            raise ValueError[day]
       
    def time2slot(hour, min):
        # 0 0000-0030
        # 1 0030-0100
        # 2 0100-0130
        # 3 0130-0200
        # 4 0200-0230
        # 46 2300-2330
        # 47 2330-0000

        slot = hour * 2
        if min == 30:
            slot = slot + 1
        return slot

        if slot > 48:
            raise Exception("Not Valid Slot")

    def slot2time(slotNum):
        if (slotNum % 2) == 1:
            hour = int(slotNum / 2)
            min = 30
            endHour = hour +1
            endMin = "00"
        else:
            hour = int(slotNum / 2)
            min = 0
            endHour = hour
            endMin = "30"
        if endHour < 10:
            endHour = "0"+str(endHour)
        else:
            endHour = str(endHour)

        if hour < 10:
            hour = "0"+str(hour)
        else:
            hour = str(hour)

        if min == 30:
            min = str(min)
        else:
            min = ("00")
        strTime = hour +min + "-" + endHour + endMin
        return strTime
