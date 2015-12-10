#!/usr/bin/env python3
from urllib.request import urlopen, Request
from urllib.parse import urlparse
import json, zipfile, csv
from pprint import pprint
from tempfile import NamedTemporaryFile

URL_BASE_ACTION = 'https://data.gov.sg/api/3/action/'
URL_BASE_HEADERS = {'User-agent':'Mozilla/5.0'}

getActionURL = {
    'package_list': lambda : ''.join([URL_BASE_ACTION, 'package_list']),
    'package_show': lambda id: ''.join([URL_BASE_ACTION, 'package_show', '?id=', id]),
    'resource_show': lambda id: ''.join([URL_BASE_ACTION, 'resource_show', '?id=', id])
}

"""
DataManager
Handles retrival of packages and resources from the gov.sg CKAN
"""
class DataManager:
    def __init__(self):
        self.cache = {}
        self.resource_cache = {}

    def _pull(self, action, *args):
        if (action, args) in self.cache:
            return self.cache[(action, args)]

        req = Request(getActionURL[action](*args), headers=URL_BASE_HEADERS)
        res = urlopen(req)
        assert(res.code == 200)
        data = res.read().decode('utf-8')
        data_dict = json.loads(data)
        if data_dict['success']:
            self.cache[(action, args)] = data_dict['result']
            return data_dict['result']
        return None

    def _getResourceData(self, resource):
        url_string = resource['url']

        if resource['id'] in self.resource_cache:
            return self.resource_cache[resource['id']]

        url = urlparse(url_string)
        req = Request(url.geturl(), headers=URL_BASE_HEADERS)
        res = urlopen(req)
        assert(res.code == 200)

        tmpfile = NamedTemporaryFile()
        tmpfile.write(res.read())

        resource_handle = tmpfile

        if url.path[-4:] == '.zip':
            try:
                resource_handle = zipfile.ZipFile(tmpfile.name, 'r')
            except:
                pass

        elif url.path[-4:] == '.csv':
            resource_handle = csv.reader(open(tmpfile.name))

        self.resource_cache[resource['id']] = resource_handle
        return resource_handle

    def getPackageList(self):
        return self._pull('package_list')

    def getPackageListDetailed(self, get_resource_data=False):
        result = {}
        for p in self.getPackageList():
            result[p] = self.getPackage(p, get_resource_data)
            print(p)
        return result

    def getPackage(self, package_name, get_resource_data=False):
        details = self._pull('package_show', package_name)
        if get_resource_data:
            for i,r in enumerate(details['resources']):
                details['resources'][i]['data'] = self._getResourceData(r)
        return details

    def getResource(self, resource_id):
        return self._pull('resource_show', resource_id)

c = DataManager()
d = c.getPackageListDetailed()
pprint(d)
