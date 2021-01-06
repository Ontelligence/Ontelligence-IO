from plaid import Client

# Available environments are 'sandbox', 'development', and 'production'.
ENVIRONMENT = 'development'

CLIENT_ID = ''
CLIENT_SECRET = ''
PUBLIC_TOKEN = ''


client = Client(client_id=CLIENT_ID, secret=CLIENT_SECRET, environment=ENVIRONMENT)

res = client.LinkToken.create({
    'user': {
        'client_user_id': '1'
    },
    'products': ["transactions"],
    'client_name': "My App",
    'country_codes': ['US'],
    'language': 'en',
    'webhook': 'https://sample.webhook.com'
})
print(res)


response = client.Transactions.get(res['link_token'], start_date='2016-07-12', end_date='2017-01-09')
transactions = response['transactions']

# the transactions in the response are paginated, so make multiple calls while increasing the offset to
# retrieve all transactions
while len(transactions) < response['total_transactions']:
    response = client.Transactions.get(ACCESS_TOKEN, start_date='2016-07-12', end_date='2017-01-09', offset=len(transactions))
    transactions.extend(response['transactions'])