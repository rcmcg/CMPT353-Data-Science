import requests
import random as rand
import json

class WikidataAPI:
    APIBaseURL = "http://www.wikidata.org/entity/"
    def get_count_sitelinks(self, qid):
        if (qid == None):
            return None
        response_json = self.__get_wikidata_query_response(qid)
        num_sitelinks = self.__count_sitelinks(response_json, qid)
        return num_sitelinks

    def __get_wikidata_query_response(self, qid):
        URL = self.APIBaseURL + str(qid).lower()
        request = requests.get(URL)
        data = request.json()
        return data

    def __count_sitelinks(self, wikidata_query, qid):
        qidKey = list(wikidata_query['entities'].items())[0][0]
        return len(wikidata_query['entities'][qidKey]['sitelinks'])