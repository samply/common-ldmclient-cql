UNMERGED BRANCH -CHANGES FROM OTHER SAMPLY TEAMS MISSING
# Samply Common LDM Client CQL

Samply Common LDM Client CQL is a library for the communication with **L**ocal **D**ata**m**anagement systems.
It extends the abstract Samply Common LDM Client.

## Features

Provides basic operations to communicate with Blaze Store.

## Build

In order to build this project, you need to configure maven properly and use the maven profile that
fits to your project.

``` 
mvn clean package
```

## Configuration

Samply Common LDM Client CQL is configured during runtime by providing the necessary paramaters in the constructor.

For usage examples, see the test classes.

## Maven artifact

To use the module, include the following artifact in your project.

``` 
<dependency>
    <groupId>de.samply</groupId>
    <artifactId>common-ldmclient.cql</artifactId>
    <version>2.0.0</version>
</dependency>
``` 

 ## License
        
 Copyright 2020 The Samply Development Community
        
 Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
        
 http://www.apache.org/licenses/LICENSE-2.0
        
 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
