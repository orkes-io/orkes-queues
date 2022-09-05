# Configuration Properties for Orkes-Conductor

## Default configuration
The default configuration is set in the [application.properties](server/src/main/resources/application.properties) that is good enough for most use cases.

### Properties that can be overridden for production use case.
Please note, the server comes with sensible defaults that works for most part, 
```properties

##########################################
##### Redis database connectivity #####
##########################################
conductor.redis-lock.serverAddress=redis://HOSTNAME:PORT
# us-east-1c is used by backward compatibility. Change this to the existing value used 
conductor.redis.hosts=HOSTNAME:PORT:us-east-1c

##########################################
##### Postgres database connectivity #####
##########################################
spring.datasource.url=jdbc:postgresql://PG_HOST:PORT/db_name

#set the user and password accordingly
spring.datasource.username=
spring.datasource.password=


##########################################
##### Misc conductor server properties
##########################################

# Enable/Disable ownerEmail field in the metadata being required
# true | false
conductor.app.owner-email-mandatory=

####################################################################################
##### Advanced performance settings -- The value shown is the default value ########
####################################################################################
# No. of threads allocated to the sweeper.  Recommended to keep 2x-3x of the total CPU count on the host
conductor.app.sweeperThreadCount=10

# Batch size for the system task worker
conductor.app.systemTaskMaxPollCount=10
# No of threads configured for the system task worker.  Recommended to be same as systemTaskMaxPollCount
conductor.app.systemTaskWorkerThreadCount=10

# Keep metadata longer in the cache to avoid excessive lookups from Redis.  Recommended value 60
# Values are in second
conductor.redis.taskDefCacheRefreshInterval=1

# Postgres connection pool size -- typically no need to change, unless you are sure
# number.  default: 8
spring.datasource.hikari.maximum-pool-size=8

```