import math

def marshalling(data):

    serialData = data[1]
    marshalled = [data[0], serialData]
    lenData = len(data)
    for i in range(start: 2, stop: lenData, step: 2):
        marshalled.append(data[i])
        marshalled.extend() #Stuck here, convert int, str, flt?
    return bytes(marshalled)

def unmarshalling(data):

    return bytes(unmarshalled)