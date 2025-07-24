import elotl
import json
import requests

@elotl.step('extraction')
def extract(context):
    response = requests.get(context.get('url'))
    if response.status_code == 200:
        return [response.json()] 
    raise RuntimeError(response.text)

@elotl.step('calculate_flags_length')
def transform(context):
    status = context.statuses.get('extraction')
    if status == elotl.success:
        for item in context.results.get('extraction'):
            item['flags_length'] = len(item['flags'])
    return context.get('extraction').get('result')    

@elotl.step
def load(context):
    with open('./result.json', 'w') as fw:
        fw.write(
            json.dumps(
                context.results.get('calculate_flags_length')
            )
        )
        fw.close()

result = elotl.execute(
    context={
        'url': 'https://restcountries.com/v3.1/all?fields=name,flags'
    },
    steps=[extract >> transform >> load]
)