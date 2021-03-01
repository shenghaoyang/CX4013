

import os
import sys
import socket
import optparse
import time
import random
import datetime

class client():
            print("Choose a service according to the following options:\n")
            print("1: Query the availability of a facility (Select days and facility)\n")
            print("2: Booking a facility (for a period of time)\n")
            print("3: Manage Booking (change booking)\n")
            print("4: Check Availability (monitor using callback)\n")
            print("5: Idempotent example\n")
            print("6: non-Idempotent example\n")

            userChoice = input('Input 1-5 or "q" to exit:')
            if userChoice == '1': 
              print("input date in year-month-day format")
              selecteddate = input()
              date_format = '%Y-%m-%d'
              try:
               date_obj = datetime.datetime.strptime(selecteddate, date_format)
               print(date_obj)
              except ValueError:
               print("Incorrect data format, should be YYYY-MM-DD")
           
               print("Select facility : 1 - meeting rooms , 2 - lecture theatres 3- study room ")
               facility = input()
              if facility == '1':
                 selectedfacility = "meeting room"
              elif facility == '2':
                 selectedfacility = "lecture theatures"
              elif facility == '3':
                 selectedfacility = "study room"
              else:
                print('You have entered an incorrect choice. please select from number 1-3')

            elif userChoice == '2':
                print("Select facility : 1 - meeting rooms , 2 - lecture theatres 3- study room ")
                facility1 = input()
                if facility1 == '1':
                 bookfacility1 = "meeting room"
                elif facility1 == '2':
                 bookfacility = "lecture theatures"
                elif facility1 == '3':
                 bookfacility = "study room"
                else:
                 print('You have entered an incorrect choice. please select from number 1-3')
                
                print("Enter start time")

                starttime = input()
                try:
                 a = datetime.datetime.strptime(starttime('specify time in HHMM format: '), "%H%M")
                 print (a.strftime("%H%M"))
                except:
                 print ("Please enter correct time in HHMM format")
                
                 print("Enter end time")

                endtime = input()
                try:
                 a = datetime.datetime.strptime(endtime('specify time in HHMM format: '), "%H%M")
                 print (a.strftime("%H%M"))
                except:
                 print ("Please enter correct time in HHMM format")
                    
                bookingid = random.sample(range(0,100 ))
                    
                print ("you have book succesfully , your Booking ID is "+ bookingid )
                
            elif userChoice == '3':
                print("Enter Booking ID")
                bookingid =input()

            else:
                print('You have entered an incorrect service.')
                print('Please input a number from 1-5 or "q" to exit.\n')





