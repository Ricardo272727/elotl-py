import time
import elotl
import random
import json

def random_wait(start = 1, end = 10):
    time.sleep(random.randint(start, end))

@elotl.step
def extract_users(context):
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
    }, {
        'id': 4,
        'name': 'Sashi',
        'company_id': 3,
    }]

@elotl.step
@elotl.depends(['extract_users'])
def transform_users(context):
    companies = {
        1: 'AWS',
        2: 'Azure',
        3: 'Google'
    }
    random_wait(1, 6)
    data = []
    for user in context['extract_users']:
        print(f"Processing user {user}") 
        data.append({
            'user_id': user['id'],
            'first_name': user['name'],
            'company': companies.get(user['company_id'], 'Unknown') 
        })
    return data

@elotl.step
@elotl.depends(['transform_users'])
def load_users(context):
    random_wait(1, 6)
    chunk = context['chunk_name']
    with open(f"results-{chunk}.json") as f:
        for user in context['transform_users']:
            f.write(user)
        f.close()
    
result = elotl.execute(
    steps=[
        extract_users,
        transform_users,
        load_users,
    ],
    config={
        'mode': elotl.PARALLEL,
    }
)