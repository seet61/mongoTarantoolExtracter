# -*- coding: utf8 -*-

from pymongo import MongoClient
import urllib, urllib2, time, calendar, json
from datetime import datetime

handler = urllib2.HTTPHandler()
opener = urllib2.build_opener(handler)
url = 'http://10.78.221.78:8800/v1/cache/create'

def getMongoData():
    client = MongoClient('mongodb://test:testpwd@10.78.221.58,10.78.221.59/?replicaSet=rs0&authSource=tokensdb&ssl=false&maxPoolSize=300')
    db = client['tokensdb']
    print db.list_collection_names()
    sps = db['servicesProfile']
    for sp in sps.find():
        send(sp)

def send(raw):
    key = raw['branchId']
    key += raw['contractName']
    key += raw['trplIdList']
    if raw['countFields'] == '4':
        key += raw['serviceList']
    value = raw['response']
    ttl = calendar.timegm(datetime.timetuple(raw['LifeTime']))
    expired = calendar.timegm(datetime.timetuple(raw['ExpireTime']))
    print key, ttl, expired
    data = {'space' : 'servicesProfile', '1' : key, '2' : value, '3' : ttl, '4' : expired}
    json_string = json.dumps(data)
    headers = {'Content-Type': "application/json"}
    req = urllib2.Request(url, json_string, headers)
    try:
        response = urllib2.urlopen(req)
        if response.code == 200 :
            print key, 'saved'
    except urllib2.HTTPError as e:
        print key, 'not saved'

def main():
    print 'start'
    getMongoData()
    print 'stop'

if __name__ == '__main__':
    main()