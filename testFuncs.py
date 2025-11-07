from calculateReturns import *

db = DatabaseManager(DATABASE_PATH)

#XIRR test
cashflows = [-600000, 200, 5000,200000, -35000, 439799]
dates = [datetime(2023,12,5), datetime(2024, 5 ,6), datetime(2024,6,7),datetime(2024,8,8),datetime(2025,5,5),datetime(2025,6,1)]
guess = 0.1
Expected = 1.37


#Descending NAV Sort Test
input = {"A": '100', "C": '300', "B": '200', "D": '400'}
expected = ["D", "C", "B",  "A"]
print(descendingNavSort(input) == expected) #should be true

#Asset 3 Visibility export Test
print(db.fetchOptions("asset3Visibility"))
