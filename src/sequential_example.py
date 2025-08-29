import elotl
import time
import json

@elotl.step
def extraction(context):
    time.sleep(3)
    return [{'d':1}, {'d': 3}, {'d': 3}] 

@elotl.step
def transform(context):
    total = 0
    for item in context['extraction']:
        total += item['d']
    return { 'total': total } 

@elotl.step
def load(context):
    with open('./result.json', 'w') as fw:
        fw.write(
            json.dumps(
                context['transform'],
                indent=4
            )
        )
        fw.close()

result = elotl.execute(
    context={
        'url': 'https://restcountries.com/v3.1/all?fields=name,flags'
    },
    steps=[extraction, transform, load],
    config={
        'mode': elotl.SEQUENTIAL,
        #'mode': 'async_dag',
        #'mode': 'parallel',
    }
)