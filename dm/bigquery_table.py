""" This template creates a BigQuery table. """


def generate_config(context):
    """ Entry point for the deployment resources. """

    name = context.properties['name']

    properties = {
        'tableReference':
            {
                'tableId': name,
                'datasetId': context.properties['datasetId'],
                'projectId': context.env['project']
            },
        'datasetId': context.properties['datasetId']
    }

    optional_properties = [
        'description',
        'friendlyName',
        'expirationTime',
        'schema',
        'timePartitioning',
        'clustering',
        'view'
    ]

    for prop in optional_properties:
        if prop in context.properties:
            if prop == 'schema':
                properties[prop] = {'fields': context.properties[prop]}
            else:
                properties[prop] = context.properties[prop]

    resources = [
        {
            'type': 'bigquery.v2.table',
            'name': name,
            'properties': properties,
            'metadata': {
                'dependsOn': [context.properties['datasetId']]
            }
        }
    ]

    outputs = [
        {
            'name': 'selfLink',
            'value': '$(ref.{}.selfLink)'.format(name)
        },
        {
            'name': 'etag',
            'value': '$(ref.{}.etag)'.format(name)
        },
        {
            'name': 'creationTime',
            'value': '$(ref.{}.creationTime)'.format(name)
        },
        {
            'name': 'lastModifiedTime',
            'value': '$(ref.{}.lastModifiedTime)'.format(name)
        },
        {
            'name': 'location',
            'value': '$(ref.{}.location)'.format(name)
        },
        {
            'name': 'numBytes',
            'value': '$(ref.{}.numBytes)'.format(name)
        },
        {
            'name': 'numLongTermBytes',
            'value': '$(ref.{}.numLongTermBytes)'.format(name)
        },
        {
            'name': 'numRows',
            'value': '$(ref.{}.numRows)'.format(name)
        },
        {
            'name': 'type',
            'value': '$(ref.{}.type)'.format(name)
        }
    ]

    return {'resources': resources, 'outputs': outputs}