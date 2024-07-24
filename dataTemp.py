# import csv

# import random

# from faker import Faker
 
# fake = Faker()
 
# # Generate a single record with realistic data

# def generate_record(record_id):

#     return {

#         "Id": record_id,

#         "EmailId": fake.email(),

#         "Name": fake.name(),

#         "Country": fake.country(),

#         "State": fake.state(),

#         "City": fake.city(),

#         "TelephoneNumber": fake.phone_number(),

#         "AddressLine1": fake.address(),

#         "AddressLine2": fake.secondary_address(),

#         "DateOfBirth": fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),

#         "FY2019_20": random.randint(1000, 1000000),

#         "FY2020_21": random.randint(1000, 1000000),

#         "FY2021_22": random.randint(1000, 1000000),

#         "FY2022_23": random.randint(1000, 1000000),

#         "FY2023_24": random.randint(1000, 1000000)

#     }
 
# # Generate and save records to a CSV file

# def generate_csv(filename, num_records):

#     fieldnames = [

#         "Id", "EmailId", "Name", "Country", "State", "City", "TelephoneNumber",

#         "AddressLine1", "AddressLine2", "DateOfBirth", "FY2019_20", "FY2020_21",

#         "FY2021_22", "FY2022_23", "FY2023_24"

#     ]
 
#     with open(filename, mode='w', newline='') as file:

#         writer = csv.DictWriter(file, fieldnames=fieldnames)

#         writer.writeheader()
 
#         for record_id in range(1, num_records + 1):

#             writer.writerow(generate_record(record_id))
 
# # Generate 100,037 records and save them to 'data.csv'

# generate_csv('inputData.csv', 1037)

 

from faker import Faker
import random
import csv
from datetime import datetime

fake = Faker()

def generate_employee_record():
    record = {
        "ID": fake.unique.random_int(min=1, max=100000),
        "Email": fake.email(),
        "Name": fake.name(),
        "Country": fake.country(),
        "State": fake.state(),
        "City": fake.city(),
        "Telephone": fake.numerify(text='###-###-####'),
        "AddressLine1": fake.street_address(),
        "AddressLine2": fake.secondary_address(),
        "DOB": fake.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d'),
        "FY2019_20": random.randint(0, 1000000),
        "FY2020_21": random.randint(0, 1000000),
        "FY2021_22": random.randint(0, 1000000),
        "FY2022_23": random.randint(0, 1000000),
        "FY2023_24": random.randint(0, 1000000)
    }
    return record

def generate_employee_data(num_records):
    data = [generate_employee_record() for _ in range(num_records)]
    return data

def save_to_csv(data, filename):
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    num_records = 10
    employee_data = generate_employee_data(num_records)
    save_to_csv(employee_data, "employee_data.csv")
    print(f"Generated {num_records} employee records and saved to 'employee_data.csv'")
