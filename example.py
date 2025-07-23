import elotl

@elotl.step
@elotl.context({
    'http_requests'
})
def extract(context):
    pass

@elotl.step
def transform(context):
    pass

@elotl.step
def load(context):
    pass

config = {
    'url': 'https://restcountries.com/v3.1/all?fields=name,flags'
}
result = elotl.configure(**config).execute(extract >> transform >> load)