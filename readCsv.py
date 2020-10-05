import csv
with open('c:\users\farag\documents\experiments.csv', 'rb') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        body = open('c:\users\farag\documents\datafiles\\'+item[3], 'rb')
        s3.Object('datacont-name', item[3]).put(Body=body)
        md = s3.Object('datacont-name', item[3]).Acl().put(ACL='public-read')

        url = " https://s3-us-west-2.amazonaws.com/datacont-name/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                         'description': item[4], 'date': item[2], 'url': url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print "item may already be there or another failure"

response = table.get_item(
    Key={
        'PartitionKey': 'experiment3',
        'RowKey': '4'
    }
)
item = response['Item']
print(item)
print(response)