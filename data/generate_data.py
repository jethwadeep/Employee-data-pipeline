# data/generate_data.py
# Generates employees_raw.csv with 1000+ records
# Intentionally includes data quality issues for the Spark cleaning pipeline

import csv
import random
from faker import Faker
from datetime import date, timedelta

fake = Faker('en_US')
random.seed(42)
Faker.seed(42)

# Config 
OUTPUT_FILE   = "data/employees_raw.csv"
TOTAL_RECORDS = 1100  # generate extra so duplicates still give 1000+

DEPARTMENTS = ['IT', 'Analytics', 'HR', 'Finance', 'Marketing',
               'Operations', 'Sales', 'Legal', 'Engineering']

JOB_TITLES  = ['Software Engineer', 'Data Analyst', 'Manager',
               'Senior Developer', 'HR Specialist', 'Financial Analyst',
               'Marketing Lead', 'Operations Manager', 'Sales Executive',
               'Data Engineer', 'DevOps Engineer', 'Product Manager']

STATUSES    = ['Active', 'active', 'ACTIVE', 'Inactive',
               'inactive', 'Terminated', 'terminated']

US_STATES   = ['NY', 'CA', 'IL', 'TX', 'FL', 'WA', 'MA',
               'GA', 'CO', 'AZ', 'OH', 'NC']

#  Helper functions 

def random_hire_date():
    """Mostly valid past dates, ~5% future dates (data error)."""
    if random.random() < 0.05:
        # Future date — intentional data error
        return date.today() + timedelta(days=random.randint(30, 900))
    return fake.date_between(start_date='-15y', end_date='today')

def random_birth_date():
    return fake.date_between(start_date='-65y', end_date='-22y')

def random_salary():
    """
    Mix of formats:
      - clean integer:       75000
      - with currency/comma: $75,000
      - float string:        75000.00
      - None (missing)
    """
    base = random.choice([
        random.randint(30000, 49999),   # Junior range
        random.randint(50000, 80000),   # Mid range
        random.randint(80001, 150000),  # Senior range
    ])
    fmt = random.random()
    if fmt < 0.05:
        return ''                                        # missing
    elif fmt < 0.35:
        return f'"${base:,}"'                           # "$75,000"
    elif fmt < 0.55:
        return str(float(base))                         # "75000.0"
    else:
        return str(base)                                # "75000"

def random_email(first, last):
    """
    Mix of valid and invalid formats, mixed case.
    """
    domains = ['company.com', 'COMPANY.COM', 'corp.org',
               'business.net', 'enterprise.io']
    fmt = random.random()
    if fmt < 0.05:
        # Invalid — missing TLD
        return f"{first.lower()}@company"
    elif fmt < 0.10:
        # Invalid — missing @
        return f"{first.lower()}.{last.lower()}company.com"
    elif fmt < 0.20:
        # Mixed case
        return f"{first.upper()}.{last.lower()}@{random.choice(domains)}"
    else:
        return f"{first.lower()}.{last.lower()}@{random.choice(domains)}"

def random_first_name():
    """Mix of cases — some all lower, some all upper, some proper."""
    name = fake.first_name()
    fmt  = random.random()
    if fmt < 0.30:
        return name.lower()
    elif fmt < 0.50:
        return name.upper()
    else:
        return name

def random_last_name():
    name = fake.last_name()
    fmt  = random.random()
    if fmt < 0.30:
        return name.upper()
    elif fmt < 0.50:
        return name.lower()
    else:
        return name

def random_department():
    """Some inconsistent casing."""
    dept = random.choice(DEPARTMENTS)
    fmt  = random.random()
    if fmt < 0.20:
        return dept.lower()
    elif fmt < 0.35:
        return dept.upper()
    return dept

def random_address():
    """~8% missing addresses."""
    if random.random() < 0.08:
        return ''
    return fake.street_address()

def random_manager_id(employee_id):
    """~10% missing manager_id (top-level employees)."""
    if random.random() < 0.10:
        return ''
    # Manager is always a lower employee_id
    if employee_id <= 1001:
        return ''
    return str(random.randint(1001, employee_id - 1))

#  Generate records 

records = []
start_id = 1001

for i in range(TOTAL_RECORDS):
    emp_id    = start_id + i
    first     = random_first_name()
    last      = random_last_name()
    hire_date = random_hire_date()
    birth_date = random_birth_date()

    record = {
        'employee_id': emp_id,
        'first_name':  first,
        'last_name':   last,
        'email':       random_email(first, last),
        'hire_date':   hire_date.strftime('%Y-%m-%d'),
        'job_title':   random.choice(JOB_TITLES),
        'department':  random_department(),
        'salary':      random_salary(),
        'manager_id':  random_manager_id(emp_id),
        'address':     random_address(),
        'city':        fake.city(),
        'state':       random.choice(US_STATES),
        'zip_code':    fake.zipcode(),
        'birth_date':  birth_date.strftime('%Y-%m-%d'),
        'status':      random.choice(STATUSES),
    }
    records.append(record)

#  Inject duplicates (~5% of records) 
# Pick random records and append them again with same employee_id
duplicate_pool = random.sample(records[:500], 55)
records.extend(duplicate_pool)

# Shuffle so duplicates aren't all at the end
random.shuffle(records)

#  Write CSV 
FIELDS = ['employee_id', 'first_name', 'last_name', 'email',
          'hire_date', 'job_title', 'department', 'salary',
          'manager_id', 'address', 'city', 'state', 'zip_code',
          'birth_date', 'status']

with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(records)

#  Summary report 
future_dates  = sum(1 for r in records
                    if r['hire_date'] > date.today().strftime('%Y-%m-%d'))
missing_salary = sum(1 for r in records if r['salary'] == '')
missing_addr   = sum(1 for r in records if r['address'] == '')
currency_fmt   = sum(1 for r in records if '$' in r['salary'])
invalid_email  = sum(1 for r in records
                     if '@' not in r['email'] or '.' not in r['email'].split('@')[-1])

print(f" Generated {OUTPUT_FILE}")
print(f"   Total rows      : {len(records)}")
print(f"   Unique IDs      : {len(set(r['employee_id'] for r in records))}")
print(f"   Duplicates      : {len(records) - len(set(r['employee_id'] for r in records))}")
print(f"   Future hire dates: {future_dates}")
print(f"   Missing salary  : {missing_salary}")
print(f"   Currency format : {currency_fmt}")
print(f"   Missing address : {missing_addr}")
print(f"   Invalid emails  : {invalid_email}")