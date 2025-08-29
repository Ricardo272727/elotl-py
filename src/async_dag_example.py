import time
import elotl
import random
import json

def random_wait(start = 1, end = 10):
    time.sleep(random.randint(start, end))

@elotl.step
def extract_customers(context):
    random_wait(1, 4)
    return [{
        'id': 1,
        'name': 'Tania',
        'company_id': 1,
    }, {
        'id': 2,
        'name': 'Joe',
        'company_id': 1,
    }, {
        'id': 3,
        'name': 'Vlam',
        'company_id': 2,
    }]

@elotl.step
def extract_companies(context):
    random_wait(1, 6)
    return [{
        'id': 1,
        'name': 'Cat Web Services',
    }, {
        'id': 2,
        'name': 'Dog Web Services',
    }]

def find_company_name(context, id):
    for company in context['extract_companies']:
        if company['id'] == id:
            return company['name']
    return None

@elotl.step
@elotl.depends(['extract_customers', 'extract_companies'])
def count_customers_by_company(context):
    customer_count = {}
    for customer in context['extract_customers']:
        company_id = customer['company_id']
        if company_id in customer_count:
            customer_count[company_id] += 1
        else:
            customer_count[company_id] = 1

    result = []
    for company_id, count in customer_count.items():
        company_name = find_company_name(context, company_id)        
        result.append({
            'company_id': company_id,
            'customers': count,
            'company_name': company_name,
        })

    return result

@elotl.step
@elotl.depends(['count_customers_by_company'])
def save_results(context):
    with open('./customer_report.json', 'w') as fw:
        fw.write(
            json.dumps(context['count_customers_by_company'], indent=4)
        )
        fw.close()

result = elotl.execute(
    steps=[
        extract_companies,
        extract_customers,
        count_customers_by_company,
        save_results,
    ],
    config={
        'mode': elotl.ASYNC_DAG,
    }
)