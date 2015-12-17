#!/usr/bin/env python3
from urllib.request import urlopen, Request
from urllib.parse import urlparse
import json, zipfile, csv
from pprint import pprint
from tempfile import NamedTemporaryFile

URL_BASE_ACTION = 'https://data.gov.sg/api/3/action'
URL_BASE_HEADERS = {'User-agent':'Mozilla/5.0'}

getActionURL = {
    'package_list': lambda : '{}/package_list'.format(URL_BASE_ACTION),
    'package_show': lambda id: '{}/package_show?id={}'.format(URL_BASE_ACTION, id),
    'resource_show': lambda id: '{}/resource_show?id={}'.format(URL_BASE_ACTION, id)
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
        with urlopen(req) as res:
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

        with urlopen(req) as res:
            assert(res.code == 200)
            data = res.read()
            tmpfile = NamedTemporaryFile()
            tmpfile.write(data)
            resource_handle = tmpfile

            if url.path[-4:].lower() == '.zip':
                resource_handle = zipfile.ZipFile(tmpfile.name, 'r')

            elif url.path[-4:].lower() == '.csv':
                resource_handle = csv.reader(data.decode('utf-8').splitlines())

            self.resource_cache[resource['id']] = resource_handle
            return resource_handle
        return None

    def getPackageList(self):
        return self._pull('package_list')

    def getPackageListDetailed(self, get_resource_data=False):
        result = {}
        for p in self.getPackageList():
            result[p] = self.getPackage(p, get_resource_data)
        return result

    def getPackage(self, package_name, get_resource_data=False):
        details = self._pull('package_show', package_name)
        if get_resource_data:
            for i,r in enumerate(details['resources']):
                details['resources'][i]['resource_handle'] = self._getResourceData(r)
        return details

    def getResource(self, resource_id):
        return self._pull('resource_show', resource_id)

c = DataManager()
"""
# Example to get resources

#package = c.getPackage('workers-made-redundant-annual', get_resource_data=True) # CSV
package = c.getPackage('wireless-hotspots', get_resource_data=True) # ZIP
pprint(package)
for resource in package['resources']:
    pprint(resource['resource_handle'].namelist())

"""

"""
# Example to get all packages with CSV resources

d = c.getPackageListDetailed()
csvresourced_packages = []
for package in d:
    for resource in d[package]['resources']:
        url_type = resource['url'][-3:].lower()
        if url_type == 'csv':
            csvresourced_packages.append(package)
            break

csvresourced_packages.sort()
pprint(csvresourced_packages)
"""

d = c.getPackageListDetailed()
data_store_active = set()
freq = set()
for package in d:
    if 'extras' in d[package]:
        for c in d[package]['extras']:
            if c['key'] == 'Frequency':
                freq.add(c['value'])
                if c['value'] == 'Daily':
                    print('Daily: {}'.format(package))
                elif c['value'] == 'Continual':
                    print('Continual: {}'.format(package))

    for resource in d[package]['resources']:
        if 'datastore_active' in resource and resource['datastore_active']:
            data_store_active.add(package)

pprint(sorted(list(freq)))
#pprint(sorted(list(data_store_active)))
