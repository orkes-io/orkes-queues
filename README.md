# Orkes Queues
Orkes Queues is a high performance queuing recipe based on the Redis as the underlying store.  

[![CI](https://github.com/orkes-io/orkes-queues/actions/workflows/ci.yaml/badge.svg)](https://github.com/orkes-io/orkes-queues/actions/workflows/ci.yml)
[![CI](https://img.shields.io/badge/license-orkes%20community%20license-green)](https://github.com/orkes-io/licenses/blob/main/community/LICENSE.txt)

<pre>
   ______   .______       __  ___  _______     _______.
 /  __  \  |   _  \     |  |/  / |   ____|   /       |
|  |  |  | |  |_)  |    |  '  /  |  |__     |   (----`
|  |  |  | |      /     |    <   |   __|     \   \    
|  `--'  | |  |\  \----.|  .  \  |  |____.----)   |   
 \______/  | _| `._____||__|\__\ |_______|_______/    
                                                      
  ______      __    __   _______  __    __   _______     _______.
 /  __  \    |  |  |  | |   ____||  |  |  | |   ____|   /       |
|  |  |  |   |  |  |  | |  |__   |  |  |  | |  |__     |   (----`
|  |  |  |   |  |  |  | |   __|  |  |  |  | |   __|     \   \    
|  `--'  '--.|  `--'  | |  |____ |  `--'  | |  |____.----)   |   
 \_____\_____\\______/  |_______| \______/  |_______|_______/    
</pre>

## Getting Started
Orkes Queues is a library that can be used to create a message broker.  Currently, the library is used as the underlying
queuing infrastructure for Orkes Conductor

### Requirements
1. Redis version 6.2+
2. The library supports Redis Standalone, Sentinel and Cluster modes
3. The library has been tested for AWS Elasticache as well as Azure Redis

### Delivery semantics
Orkes Queues provides `at-least once` delivery semantics.

### Using Library

#### Published Artifacts
Server Jar is published on maven central at the following location:
[TBD](http://orkes.io)

### Contributions
We welcome community contributions and PRs to this repository.

### Get Support 
Use GitHub issue tracking for filing issues and Discussion Forum for any other questions, ideas or support requests.
Orkes (http://orkes.io) development team creates and maintains the Orkes-Conductor releases.

### License
Copyright 2022 Orkes, Inc
Licensed under Orkes Community License.  You may obtain a copy of the License at:
```
https://github.com/orkes-io/licenses/blob/main/community/LICENSE.txt
```
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.