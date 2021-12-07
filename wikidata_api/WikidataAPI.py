import requests
import random as rand
import json

class WikidataAPI:
    APIBaseURL = "http://www.wikidata.org/entity/"
    def get_count_sitelinks(self, qid):
        if (qid == None):
            return None
        print("Going to send request with qid " + qid)
        response_json = self.__get_wikidata_query_response(qid)
        # print("response_json")
        # print(response_json)
        num_sitelinks = self.__count_sitelinks(response_json, qid)
        return num_sitelinks
        # return rand.randint(1,100)

    def __get_wikidata_query_response(self, qid):
        URL = self.APIBaseURL + str(qid).lower()
        print("Sending request to " + URL)
        request = requests.get(URL)
        print("Status code: " + str(request.status_code))
        # print("Request raw: ")
        # print(request.content)
        data = request.json()
        return data

    def __count_sitelinks(self, wikidata_query, qid):
        # print("__count_sitelinks")
        # print("data")
        # print("__count_sitelinks", wikidata_query.content)
        # subkey = wikidata_query['entities'].keys()
        qidKey = list(wikidata_query['entities'].items())[0][0]
        # print(qidKey)
        return len(wikidata_query['entities'][qidKey]['sitelinks'])