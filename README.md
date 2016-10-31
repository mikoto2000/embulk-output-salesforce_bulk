# Salesforce Bulk output plugin for Embulk

Salesforce Bulk API で Upsert を行います。


## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: no


## Configuration

- **userName**: Salesforce user name.(string, required)
- **password**: Salesforce password.(string, required)
- **authEndpointUrl**: Salesforce login endpoint URL.(string, required)
- **objectType**: object type of JobInfo.(string, required)
- **externalId**: external id of JobInfo.(string, required)
- **isCompression**: compression setting(true: compress, false: no compress) of JobInfo.(boolean, default is true)
- **workingDirectory**: working directory, this plugin create temp files in this directory.(string, required)
- **pollingIntervalMillisecond**: polling interval millisecond.(string, default is 30000)


## Example

```yaml
out:
  type: salesforce_bulk
  userName: USER_NAME
  password: PASSWORD
  authEndpointUrl: https://login.salesforce.com/services/Soap/u/34.0
  objectType: Account
  externalId: Name
  isCompression: true
  workingDirectory: ./tmp
  pollingIntervalMillisecond: 5000
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
