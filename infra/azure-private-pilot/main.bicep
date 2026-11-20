targetScope = 'subscription'

param sshPublicKey string

var location = 'canadacentral'
var name = 't3-private-pilot'

resource group 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: 'rg-${name}-${location}'
  location: location
  tags: {
    workload: name
  }
}

module pilot './vm.bicep' = {
  name: name
  scope: group
  params: {
    name: name
    location: location
    sshPublicKey: sshPublicKey
  }
}

output vmId string = pilot.outputs.vmId
