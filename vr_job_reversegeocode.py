import json
import aiohttp
import asyncio
import backoff
import logging
import requests
import pandas as pd
from dateutil import parser

logging.getLogger("backoff").addHandler(logging.StreamHandler())
def fatal_code(e: Exception) -> None:
    return None

class VRJobLatLong:
    """
    Class that replicates the inputs and outputs of Venue Replay
    To provide results from Atlas instead of OnSpot
    """
    def __init__(
            self,latitude,longitude
    ):

        self.req_headers = {'accept-encoding': 'gzip'}
        self.url = "https://atlas.k8s.eltoro.com/"
        self.ref_id = "vr-prototype"
        
        self.address_match = []
        self.ethash_addr_match = []
        self.latitude = latitude
        self.longitude = longitude
        
    @backoff.on_exception(
        backoff.expo,
        (
            aiohttp.ClientError,
            aiohttp.ClientOSError,
            aiohttp.ClientConnectorError,
            asyncio.TimeoutError,
            json.decoder.JSONDecodeError,
            requests.exceptions.ConnectionError
        ),
        max_tries=4,
        max_time=20,
        jitter=None,
        factor=2,
        raise_on_giveup=False,
        giveup=fatal_code
    )
    def _address_for_latlong_query(self):   
        q = '{{ reverseGeocode(latitude: {}, \
        longitude: {}) \
        {{address, etHash}}}}'.format(self.latitude, self.longitude)
   
        response = requests.post(self.url, json={'query': q}, headers=self.req_headers)
        #print(response)
        return response.json()

    def get_address_for_latlong(self):
        
        response = self._address_for_latlong_query()
        address = response['data']['reverseGeocode'][0]['address']
        etHash = response['data']['reverseGeocode'][0]['etHash']
        self.address_match = address
        self.ethash_addr_match = etHash
        
        #return address,etHash
        
    @backoff.on_exception(
        backoff.expo,
        (
            aiohttp.ClientError,
            aiohttp.ClientOSError,
            aiohttp.ClientConnectorError,
            asyncio.TimeoutError,
            json.decoder.JSONDecodeError,
            requests.exceptions.ConnectionError
        ),
        max_tries=4,
        max_time=20,
        jitter=None,
        factor=2,
        raise_on_giveup=False,
        giveup=fatal_code
    )
    def run_job(self):
        self.get_address_for_latlong()
        #try:
            #self.get_address_for_latlong()
        #except requests.exceptions.ConnectionError as e:
            #print(f"ConnectionError: {e}")
        #except Exception as e:
            #print(f"An error occurred: {e}")
