def GenerateConfig(context):
  # Creates a google cloud storage bucket with properties from the YAML file
  resources = [{
      'name': context.env['name'],
      'type': 'storage.v1.bucket',
      'properties': {
          'location': context.properties['location'],
          'locationType': context.properties['locationType'],
          'storageClass': context.properties['storageClass']
      }
  }]
  return {'resources': resources}