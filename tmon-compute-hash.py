import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch()

def getDateRange():
	DateRange=[]
	DateRangeMax=''
	DateRangeMin=''
	indexDateMasqMax = str("tmonthreads-" + datetime.datetime.now().strftime("%Y.%m.%d"))
	try:
		resMax = es.search(index=indexDateMasqMax , doc_type="tmon-threads" ,  body={ "size": 0,  "aggs":{  "max_date":{"max":{"field": "date"}}}})
		DateRangeMax = resMax['aggregations']['max_date']['value_as_string']
	except:
		pass	

	indexDateMasqMin = "heavyhashes"
	try:
		resMin = es.search(index=indexDateMasqMin , doc_type="hashes" ,  body={ "size": 0,  "aggs":{  "max_date":{"max":{"field": "date"}}}})
		DateRangeMin = resMin['aggregations']['max_date']['value_as_string']
	except:
		DateRangeMin = '1970-01-01T00:00:00Z'

	DateRange=[DateRangeMax,DateRangeMin]
	return DateRange

def getHashList():
	HashList = []
	indexDateMasq = str("tmonthreads-" + datetime.datetime.now().strftime("%Y.%m.%d"))
	dateRangeMax=getDateRange()[0]
	dateRangeMin=getDateRange()[1]		
	#try:
	bodytext = '{"size": 0,"query":{"range":{"date":{"gte":"'+ dateRangeMin + '" ,"lte": "' + dateRangeMax + '" }}},"aggs": {"distinct_hash_value": {"terms": {"field":"hash" },"aggs": {"distinct_server_value": {"terms": { "field":"server" },"aggs": {"distinct_thread_value": {"terms": {"field":"thread_id" }}}}}}}}'
	res = es.search(index=indexDateMasq , doc_type="tmon-threads" ,  body=bodytext )
	for hash in res['aggregations']['distinct_hash_value']['buckets']:
		hash_id = hash['key']
		for server in hash['distinct_server_value']['buckets']:
			server_id = server['key']
			for thread in server['distinct_thread_value']['buckets']:
				thread_id = thread['key']
				HashList.append([hash_id,server_id,thread_id])	
	return HashList


def computeHashTimeTaken():
	HashTimeTakenList=[]
	indexDateMasq = str("tmonthreads-" + datetime.datetime.now().strftime("%Y.%m.%d"))
	for hashtimetaken in getHashList():
		bodytextMin = '{"size":0,"query": {"bool": {"must":[{"match": {"hash": "'+hashtimetaken[0]+'"}},{"match": {"server": "'+hashtimetaken[1]+'"}},{"match": {"thread_id": "'+hashtimetaken[2]+'"}}]}},"aggs": {"min_time_taken": {"min": {"field" : "time_taken"}}}}'
		bodytextMax = '{"size":0,"query": {"bool": {"must":[{"match": {"hash": "'+hashtimetaken[0]+'"}},{"match": {"server": "'+hashtimetaken[1]+'"}},{"match": {"thread_id": "'+hashtimetaken[2]+'"}}]}},"aggs": {"max_time_taken": {"max": {"field" : "time_taken"}}}}'
		resMinTimeTaken = es.search(index=indexDateMasq , doc_type="tmon-threads" ,  body=bodytextMin)	
		resMaxTimeTaken = es.search(index=indexDateMasq , doc_type="tmon-threads" ,  body=bodytextMax)
		bodytextMinTimestamp='{"size":0,"query": {"bool": {"must":[{"match": {"hash": "'+hashtimetaken[0]+'"}},{"match": {"server": "'+hashtimetaken[1]+'"}},{"match": {"thread_id": "'+hashtimetaken[2]+'"}}]}},"aggs": {"min_date": {"min": {"field" : "date"}}}}'
		bodytextMaxTimestamp='{"size":0,"query": {"bool": {"must":[{"match": {"hash": "'+hashtimetaken[0]+'"}},{"match": {"server": "'+hashtimetaken[1]+'"}},{"match": {"thread_id": "'+hashtimetaken[2]+'"}}]}},"aggs": {"max_date": {"max": {"field" : "date"}}}}'		
		resMinTimestamp = es.search(index=indexDateMasq , doc_type="tmon-threads" ,  body=bodytextMinTimestamp)
		resMaxTimestamp = es.search(index=indexDateMasq , doc_type="tmon-threads" ,  body=bodytextMaxTimestamp)
		minTimeTaken=resMinTimeTaken['aggregations']['min_time_taken']['value']
		maxTimeTaken=resMaxTimeTaken['aggregations']['max_time_taken']['value']
                minDate=resMinTimestamp['aggregations']['min_date']['value_as_string']
                maxDate=resMaxTimestamp['aggregations']['max_date']['value_as_string']		
		HashTimeTakenList.append([hashtimetaken[0],hashtimetaken[1],hashtimetaken[2],minDate,maxDate,int(maxTimeTaken)-int(minTimeTaken)])

	return HashTimeTakenList

def insertHeavyHashes():

	for doc in computeHashTimeTaken():
		doc_hash = '{ "hash":"'+doc[0]+'","server":"'+doc[1]+'","thread_id":"'+doc[2]+'","time_begin":"'+doc[3]+'","time_end":"'+doc[4]+'","time_taken":"'+str(doc[5])+'"}'
		print doc_hash
		res = es.index(index="heavyhashes", doc_type='hashes', id=doc[0] ,  body=doc_hash)
		print(res['created'])

if __name__=="__main__":
#	for i in getHashList():
#		print i
#	for i in computeHashTimeTaken():
#		print i
	insertHeavyHashes()
