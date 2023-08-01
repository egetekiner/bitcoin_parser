# Final Version

#ToDO: In/Out - Unique listing and finalizing
#ToDo: Figure out how to get more than 5000 transactions



import requests
from typing import List
import json
import time
import pandas as pd
import datetime

def parse_bitcoin(wallet_address):
    start_time = time.time()


    address = wallet_address

    f = requests.get(f"https://blockchain.info/rawaddr/{address}?limit=7500")

    # API status code error handling.
    if f.status_code != 200:
        print(f.status_code)


    f_result = f.json()

    f_result

    i = 0

    print("\n","total ",len(f_result['txs']), "transactions imported in %s minutes" % str(datetime.timedelta(seconds=(time.time() - start_time))))        

    #Parsing part

    df = pd.DataFrame(columns = ['Tx_Hash', 'Sender_address','in/out','Trx_fee','TimeStamp','Value'])
    print(len(f_result['txs']))
    i=0
    while i in range(len(f_result['txs'])):


        Tx_Hash = f_result['txs'][i]['hash'] #unchanged variable for every transaction
        TimeStamp =  f_result['txs'][i]['time'] #unchanged variable for every transaction
        Trx_fee = f_result['txs'][i]['fee'] #unchanged variable for every transaction

        Sender_address = 'asd'
        Value = ''
        in_out = ''

        if len(f_result['txs'][i]['inputs']) == 1:

            if f_result['txs'][i]['inputs'][0]['prev_out']['addr'] == address:

                Sender_address = address
                in_out = 'out'
                Value = f_result['txs'][i]['inputs'][0]['prev_out']['value']

            else:

                Sender_address = f_result['txs'][i]['inputs'][0]['prev_out']['addr']
                in_out = 'in'

                total_in = 0
                x=0
                while x in range(len(f_result['txs'][i]['out'])):

                    if  'addr' in f_result['txs'][i]['out'][x]:
                        if f_result['txs'][i]['out'][x]['addr'] == address:
                            total_in += f_result['txs'][i]['out'][x]['value']
                    x += 1

                Value = total_in


        else: #if there are more than one input

            x = 0
            total_out = 0
            validation = False


            while x in range(len(f_result['txs'][i]['inputs'])):

                if f_result['txs'][i]['inputs'][x]['prev_out']['addr'] == address:

                    total_out += f_result['txs'][i]['inputs'][x]['prev_out']['value']
                    validation = True

                if validation == True:        
                    Sender_address = address
                    in_out = 'out'
                    Value = total_out

                else:

                    total_in = 0
                    y=0

                    while y in range(len(f_result['txs'][i]['out'])):

                        if  'addr' in f_result['txs'][i]['out'][y]:
                            if f_result['txs'][i]['out'][y]['addr'] == address:
                                total_in += f_result['txs'][i]['out'][y]['value']
                        y += 1

                    Value = total_in
                    Sender_address = 'Multiple Address'
                    in_out = 'in'

                x += 1



        i += 1

        print(i, ' --- ', Tx_Hash, ' -- ',Trx_fee )         
        trx_list = [Tx_Hash,Sender_address,in_out,(Trx_fee/(100000000)),TimeStamp,(Value/(100000000))]
        df.loc[len(df)] = trx_list
        
    return df

#Basic request for an address

df = parse_bitcoin('<your address here>')

print(df)
