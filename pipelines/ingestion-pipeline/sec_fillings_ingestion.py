import logging
from datetime import timedelta
import re
import xml.etree.ElementTree as ET
import json
from typing import Dict

import pandas as pd
import requests
from bytewax import operators as op
from bytewax.connectors.files import FileSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import SimplePollingSource
from bytewax.run import cli_main
# from bytewax.connectors.kafka import operators as kop
# from bytewax.connectors.kafka import KafkaSinkMessage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def log_item(item):
    logging.info(f"Parsed Filing: {item}")
    return item

def serialize_for_output(item):
    """Convert the item to JSON string for file output"""
    key, value = item
    json_string = json.dumps(value)
    return (key, json_string) 


class SECSource(SimplePollingSource):
    def next_item(self):
        base_url = "https://www.sec.gov/cgi-bin/browse-edgar"

        headers = {
            'User-Agent': 'ssc_filling_ingestion abt@myyhoo.com',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Cache-Control':'no-cache',
            'Host': 'www.sec.gov'
        }

        params = {
            'action': 'getcurrent',
            'CIK': '',
            'type': '',
            'dateb': '',
            'owner': 'include',
            'start': '0',
            'count': '200',
            'output': 'atom'
        }

        response = requests.get(base_url, headers=headers, params=params)

        if response.status_code == 200:
            logger.info('Successfully retrieved the fillings')
            return response.text
        else:
            logger.info(f'Failed to retrieve the filings. Status code {response.status_code}')
            return None
        

def parse_atom(xml_data):
    namespace = {'atom': 'http://www.w3.org/2005/Atom'}
    
    root = ET.fromstring(xml_data)
    data = []

    for entry in root.findall('atom:entry', namespace):
        id = entry.find('atom:id', namespace).text.split('=')[-1].replace('-', '')
        title = entry.find('atom:title', namespace).text
        link = entry.find("atom:link[@type='text/html']", namespace).get("href")
        cik_match = re.search(r'\((\d+)\)',title)
        cik = cik_match.group(1) if cik_match else 'No CIK found'
        category_elem = entry.find('atom:category', namespace)
        form_type = category_elem.get('term') if category_elem is not None else 'Unknown'

        data.append(('ALL',{

            'id': id,
            'title': title,
            "link": link,
            "cik": cik,
            'form_type':form_type
        }))

    return data


def dedupe(filings, filing):
    if filing is None:
        # just return current state, no output
        return (filings, None)

    if filings is None:
        filings = []

    if filing['id'] in filings:
        return (filings, None)
    else:
        filings.append(filing['id'])
        return (filings, filing)


if __name__ == '__main__':
    flow = Dataflow("edgar_scraper")
    filings_stream = op.input("in", flow, SECSource(timedelta(seconds=10)))
    processed_stream = op.flat_map("parse_atom", filings_stream, parse_atom)
    logged_stream = op.map("log_each", processed_stream, log_item)
    
    deduped_stream = op.stateful_map("dedupe", processed_stream, dedupe)
    op.inspect("dedupe_stream", deduped_stream)
    deduped_filtered_stream = op.filter_map("remove_key", deduped_stream, lambda x: x[1])
    op.inspect('filt', deduped_filtered_stream)
    # cik_to_ticker = pd.read_json("company_tickers.json", orient="index")
    # cik_to_ticker.set_index(cik_to_ticker["cik_str"], inplace=True)

    # Convert to JSON strings for file output
    serialized_stream = op.map("serialize", logged_stream, serialize_for_output)
    
    # Add an output sink
    op.output("out", serialized_stream, FileSink("output.jsonl"))
    
    # Use cli_main for execution
    cli_main(flow)