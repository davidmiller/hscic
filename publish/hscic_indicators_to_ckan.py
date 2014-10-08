"""
Publish HSCIC Indicators to CKAN !
"""
import sys

import ffs

import dc

DATA_DIR = ffs.Path.here()/'../data'


def publish_indicators():
    indicatorfile = DATA_DIR/'indicators.json'
    indicators = indicatorfile.json_load()
    for indicator in indicators:
        resources = [
            dict(
                description=s['description'],
                name=s['url'].split('/')[-1],
                format=s['filetype'],
                upload=dc.fh_for_url(s['url'])
            )
            for s in indicator['sources']
        ]
        dc.Dataset.create_or_update(
            name=indicator['unique identifier'].lower(),
            title=indicator['title'],
            state='active',
            licence_id='ogl',
            notes=indicator['unique identifier'],
            url='https://indicators.ic.nhs.uk/webview/',
            tags=dc.tags(*indicator['keyword(s)']),
            resources=resources,
            owner_org='hscic'
        )
        print 'Published', indicator['unique identifier']
    return
    
def main():
    dc.ensure_publisher('hscic')
    publish_indicators()
    return 0

if __name__ == '__main__':
    sys.exit(main())
