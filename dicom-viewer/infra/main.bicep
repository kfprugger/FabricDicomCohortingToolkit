// Deploy OHIF Viewer (Static Web App) + DICOMweb Proxy (Container App)
// JIT architecture: DICOM files stay in OneLake, fetched on-demand
// All open-source: OHIF (MIT), proxy (custom Python/Flask container)

@description('Azure region for compute resources')
param location string = resourceGroup().location

@description('Azure region for Static Web App (not available in all regions)')
param swaLocation string = 'westus2'

@description('Base name for resources')
param baseName string = 'dicom'

@description('Fabric SQL analytics endpoint server (for runtime index refresh)')
param fabricSqlServer string = ''

@description('Fabric SQL database name (Silver Lakehouse)')
param fabricSqlDatabase string = ''

@description('Static Web App SKU')
@allowed(['Free', 'Standard'])
param swaSku string = 'Free'

@description('ACR name for proxy container image')
param acrName string = '${replace(baseName, '-', '')}acr'

// Log Analytics Workspace
resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: '${baseName}-logs'
  location: location
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

// Azure Container Registry
resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: length(acrName) > 50 ? substring(acrName, 0, 50) : acrName
  location: location
  sku: { name: 'Basic' }
  properties: {
    adminUserEnabled: true
  }
}

// Container Apps Environment
resource containerEnv 'Microsoft.App/managedEnvironments@2023-05-01' = {
  name: '${baseName}-env'
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
  }
}

// DICOMweb Proxy Container App
resource proxy 'Microsoft.App/containerApps@2023-05-01' = {
  name: '${baseName}-proxy'
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    managedEnvironmentId: containerEnv.id
    configuration: {
      ingress: {
        external: true
        targetPort: 8080
        transport: 'http'
        corsPolicy: {
          allowedOrigins: ['*']
          allowedMethods: ['GET', 'OPTIONS']
          allowedHeaders: ['*']
          maxAge: 3600
        }
      }
      registries: [
        {
          server: acr.properties.loginServer
          username: acr.listCredentials().username
          passwordSecretRef: 'acr-password'
        }
      ]
      secrets: [
        {
          name: 'acr-password'
          value: acr.listCredentials().passwords[0].value
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'proxy'
          image: '${acr.properties.loginServer}/${baseName}-proxy:latest'
          resources: {
            cpu: json('0.5')
            memory: '1.0Gi'
          }
          env: [
            {
              name: 'DICOM_INDEX_PATH'
              value: '/app/dicom_index.json'
            }
            {
              name: 'FABRIC_SQL_SERVER'
              value: fabricSqlServer
            }
            {
              name: 'FABRIC_SQL_DATABASE'
              value: fabricSqlDatabase
            }
          ]
        }
      ]
      scale: {
        minReplicas: 0
        maxReplicas: 3
        rules: [
          {
            name: 'http-rule'
            http: {
              metadata: {
                concurrentRequests: '10'
              }
            }
          }
        ]
      }
    }
  }
}

// Static Web App for OHIF Viewer
resource swa 'Microsoft.Web/staticSites@2022-09-01' = {
  name: '${baseName}-ohif'
  location: swaLocation
  sku: {
    name: swaSku
  }
  properties: {
    buildProperties: {
      appLocation: '/'
      outputLocation: 'dist'
    }
  }
}

// Outputs
output proxyUrl string = 'https://${proxy.properties.configuration.ingress.fqdn}'
output proxyName string = proxy.name
output acrLoginServer string = acr.properties.loginServer
output acrName string = acr.name
output ohifSwaName string = swa.name
output ohifSwaDefaultHostname string = swa.properties.defaultHostname
