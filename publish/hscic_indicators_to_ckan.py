"""
Publish HSCIC Indicators to CKAN !
"""
import logging
import sys
import ffs
import dc


logging.basicConfig(filename='publish.log',
                    format='%(asctime)s %(levelname)s: %(message)s',
                    level=logging.DEBUG)


DATA_DIR = ffs.Path.here()/'../data'


def publish_indicators(start_from=0):
    indicatorfile = DATA_DIR/'indicators.json'
    logging.info('Loading {}'.format(indicatorfile))
    indicators = indicatorfile.json_load()
    amount = len(indicators)
    logging.info('Processing {} indicators'.format(amount))
    logging.info('Starting from record {}'.format(start_from))
    for indicator in indicators[start_from:10]:
        logging.info('{} of {}'.format(start_from, amount))
        start_from += 1
        try:
            logging.info('Processing {}'.format(indicator['title']))
            logging.info('ID: {}'.format(indicator['unique identifier'].lower()))
            resources = [
                dict(
                    description=s['description'],
                    name=s['url'].split('/')[-1],
                    format=s['filetype'],
                    upload=dc.fh_for_url(s['url'])
                )
                for s in indicator['sources']
            ]
            name = 'hscic_indicator_{}'.format(indicator['unique identifier'].lower())
            # Metadata specified by NHSEngland identified in comments...
            dc.Dataset.create_or_update(
                name=name, # Unique ID
                title=indicator['title'], #title
                notes=indicator['definition'], # description
                tags=dc.tags(*indicator['keyword(s)']), # tags
                extras=[
                    {'key': 'Public Access Level',
                     'value': 'Public',},
                    {'key': 'Data Quality Assurance',
                     'value': 'False'},
                    {'key': 'Release Date',
                     'value': indicator['current version uploaded'],},
                    {'key': 'Status',
                     'value': 'Live',},
                ],
                state='active',
                licence_id='ogl',
                url='https://indicators.ic.nhs.uk/webview/',
                resources=resources,
                groups=[
                    {'name': 'indicators'},
                ],
                owner_org='hscic' # publisher
            )
        except Exception as ex:
            logging.error(ex)
    return

def publish_datasets(start_from=0):
    datasetfile = DATA_DIR/'datasets.json'
    logging.info('Loading {}'.format(datasetfile))
    datasets = datasetfile.json_load()
    amount = len(datasets)
    logging.info('Processing {} datasets'.format(amount))
    logging.info('Starting from record {}'.format(start_from))
    for dataset in datasets[start_from:10]:
        try:
            logging.info('{} of {}'.format(start_from, amount))
            start_from += 1
            logging.info('Processing {}'.format(dataset['title']))
            logging.info('ID: {}'.format(dataset['id']))
            resources = [
                dict(
                    description=s['description'],
                    name=s['url'].split('/')[-1],
                    format=s['filetype'],
                    upload=dc.fh_for_url(s['url'])
                )
                for s in dataset['sources']
            ]
            notes = dataset['summary']
            if 'key_facts' in dataset:
                notes += '\n\nKEY FACTS:\n==========\n\n' + dataset['key_facts']
            extras = [
                {'key': 'Public Access Level',
                 'value': 'Public',},
                {'key': 'Data Quality Assurance',
                 'value': 'False'},
                {'key': 'Status',
                 'value': 'Live',},
            ]
            if 'date_range' in dataset:
                extras.append({
                    'key': 'Time period',
                    'value': dataset['date_range'],
                })
            if 'publication_date' in dataset:
                extras.append({
                    'key': 'Release date',
                    'value': dataset['publication_date'],
                })
            if 'geographical_coverage' in dataset:
                extras.append({
                    'key': 'Geographical coverage',
                    'value': ', '.join(dataset['geographical_coverage'])
                })
            # groups
            groups = []
            for item in dataset['topics']:
                groups.append(item)
            for item in dataset['information_types']:
                groups.append(item)
            group_faff = []
            for g in groups:
                group_name = dc.ensure_group(g, 'HSCIC')
                group_faff.append({
                    'name': group_name,
                })
            name = 'hscic_dataset_{}'.format(dataset['id'])
            # NHSEngland metadata as comments...
            dc.Dataset.create_or_update(
                name=name, # Unique ID
                title=dataset['title'], # title
                notes=notes, # description
                tags=dc.tags(*dataset['keywords']), # tags
                extras=extras,
                state='active',
                licence_id='ogl',
                url=dataset['source'],
                resources=resources,
                groups=group_faff,
                owner_org='hscic' # publisher
            )
        except Exception as ex:
            logging.error(ex)
    return

def main():
    dc.ensure_publisher('hscic')
    dc.ensure_group('indicators', 'hscic')
    publish_indicators()
    publish_datasets()
    return 0

if __name__ == '__main__':
    sys.exit(main())
