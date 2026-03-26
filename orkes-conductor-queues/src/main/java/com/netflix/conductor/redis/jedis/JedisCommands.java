/*
 * Copyright 2026 Orkes, Inc.
 * <p>
 * Licensed under the Orkes Community License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * https://github.com/orkes-io/licenses/blob/main/community/LICENSE.txt
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.redis.jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

/** Common interface for sharded and non-sharded Jedis. */
public interface JedisCommands {

    /**
     * Sets a string value for the given key.
     *
     * @param key the key
     * @param value the value
     * @return status code reply
     */
    String set(String key, String value);

    /**
     * Sets a string value for the given key with additional parameters.
     *
     * @param key the key
     * @param value the value
     * @param params set parameters (e.g. EX, PX, NX, XX)
     * @return status code reply
     */
    String set(String key, String value, SetParams params);

    /**
     * Sets a byte array value for the given key with additional parameters.
     *
     * @param key the key
     * @param value the value
     * @param params set parameters
     * @return status code reply
     */
    String set(byte[] key, byte[] value, SetParams params);

    /**
     * Gets the value for the given key.
     *
     * @param key the key
     * @return the value, or null if the key does not exist
     */
    String get(String key);

    /**
     * Checks if the given key exists.
     *
     * @param key the key
     * @return true if the key exists
     */
    Boolean exists(String key);

    /**
     * Removes the expiration from a key.
     *
     * @param key the key
     * @return 1 if the timeout was removed, 0 if the key does not exist or has no timeout
     */
    Long persist(String key);

    /**
     * Sets an expiration on a key in seconds.
     *
     * @param key the key
     * @param seconds the expiration time in seconds
     * @return 1 if the timeout was set, 0 if the key does not exist
     */
    Long expire(String key, long seconds);

    /**
     * Returns the remaining time to live of a key in seconds.
     *
     * @param key the key
     * @return TTL in seconds, -2 if the key does not exist, -1 if no expiry is set
     */
    Long ttl(String key);

    /**
     * Sets the value only if the key does not exist.
     *
     * @param key the key
     * @param value the value
     * @return 1 if the key was set, 0 if the key already exists
     */
    Long setnx(String key, String value);

    /**
     * Increments the integer value of a key by the given amount.
     *
     * @param key the key
     * @param increment the increment value
     * @return the new value after incrementing
     */
    Long incrBy(String key, long increment);

    /**
     * Sets the value of a hash field.
     *
     * @param key the key
     * @param field the field
     * @param value the value
     * @return 1 if the field is new, 0 if it was updated
     */
    Long hset(String key, String field, String value);

    /**
     * Sets multiple hash fields at once.
     *
     * @param key the key
     * @param hash map of field-value pairs
     * @return the number of fields added
     */
    Long hset(String key, Map<String, String> hash);

    /**
     * Gets the value of a hash field.
     *
     * @param key the key
     * @param field the field
     * @return the value, or null if the field does not exist
     */
    String hget(String key, String field);

    /**
     * Sets the value of a hash field only if it does not exist.
     *
     * @param key the key
     * @param field the field
     * @param value the value
     * @return 1 if the field was set, 0 if it already exists
     */
    Long hsetnx(String key, String field, String value);

    /**
     * Increments the integer value of a hash field by the given amount.
     *
     * @param key the key
     * @param field the field
     * @param value the increment value
     * @return the new value after incrementing
     */
    Long hincrBy(String key, String field, long value);

    /**
     * Checks if a hash field exists.
     *
     * @param key the key
     * @param field the field
     * @return true if the field exists
     */
    Boolean hexists(String key, String field);

    /**
     * Deletes one or more hash fields.
     *
     * @param key the key
     * @param field the fields to delete
     * @return the number of fields removed
     */
    Long hdel(String key, String... field);

    /**
     * Returns the number of fields in a hash.
     *
     * @param key the key
     * @return the number of fields
     */
    Long hlen(String key);

    /**
     * Returns all values in a hash.
     *
     * @param key the key
     * @return list of values
     */
    List<String> hvals(String key);

    /**
     * Adds one or more members to a set.
     *
     * @param key the key
     * @param member the members to add
     * @return the number of members added
     */
    Long sadd(String key, String... member);

    /**
     * Returns all members of a set.
     *
     * @param key the key
     * @return set of members
     */
    Set<String> smembers(String key);

    /**
     * Removes one or more members from a set.
     *
     * @param key the key
     * @param member the members to remove
     * @return the number of members removed
     */
    Long srem(String key, String... member);

    /**
     * Returns the number of members in a set.
     *
     * @param key the key
     * @return the set cardinality
     */
    Long scard(String key);

    /**
     * Checks if a member exists in a set.
     *
     * @param key the key
     * @param member the member to check
     * @return true if the member exists
     */
    Boolean sismember(String key, String member);

    /**
     * Adds a member to a sorted set with the given score.
     *
     * @param key the key
     * @param score the score
     * @param member the member
     * @return 1 if the member was added, 0 if it was updated
     */
    Long zadd(String key, double score, String member);

    /**
     * Adds multiple members to a sorted set with their scores.
     *
     * @param key the key
     * @param scores map of member-score pairs
     * @return the number of members added
     */
    Long zadd(String key, Map<String, Double> scores);

    /**
     * Adds a member to a sorted set with additional parameters.
     *
     * @param key the key
     * @param score the score
     * @param member the member
     * @param params additional parameters (e.g. NX, XX, CH)
     * @return the number of elements added or updated
     */
    Long zadd(String key, double score, String member, ZAddParams params);

    /**
     * Returns a range of members from a sorted set by index.
     *
     * @param key the key
     * @param start the start index
     * @param stop the stop index
     * @return list of members
     */
    List<String> zrange(String key, long start, long stop);

    /**
     * Removes one or more members from a sorted set.
     *
     * @param key the key
     * @param members the members to remove
     * @return the number of members removed
     */
    Long zrem(String key, String... members);

    /**
     * Returns a range of members with scores from a sorted set by index.
     *
     * @param key the key
     * @param start the start index
     * @param stop the stop index
     * @return list of tuples containing member and score
     */
    List<Tuple> zrangeWithScores(String key, long start, long stop);

    /**
     * Returns the number of members in a sorted set.
     *
     * @param key the key
     * @return the sorted set cardinality
     */
    Long zcard(String key);

    /**
     * Returns the number of members in a sorted set within the given score range.
     *
     * @param key the key
     * @param min the minimum score
     * @param max the maximum score
     * @return the count of members in the score range
     */
    Long zcount(String key, double min, double max);

    /**
     * Returns the score of a member in a sorted set.
     *
     * @param key the key
     * @param member the member
     * @return the score, or null if the member does not exist
     */
    Double zscore(String key, String member);

    /**
     * Returns members in a sorted set within the given score range.
     *
     * @param key the key
     * @param min the minimum score
     * @param max the maximum score
     * @return list of members
     */
    List<String> zrangeByScore(String key, double min, double max);

    /**
     * Returns members in a sorted set within the given score range with offset and count.
     *
     * @param key the key
     * @param min the minimum score
     * @param max the maximum score
     * @param offset the starting offset
     * @param count the maximum number of members to return
     * @return list of members
     */
    List<String> zrangeByScore(String key, double min, double max, int offset, int count);

    /**
     * Returns members with scores in a sorted set within the given score range.
     *
     * @param key the key
     * @param min the minimum score
     * @param max the maximum score
     * @return list of tuples containing member and score
     */
    List<Tuple> zrangeByScoreWithScores(String key, double min, double max);

    /**
     * Removes members in a sorted set within the given score range.
     *
     * @param key the key
     * @param min the minimum score
     * @param max the maximum score
     * @return the number of members removed
     */
    Long zremrangeByScore(String key, String min, String max);

    /**
     * Deletes a key.
     *
     * @param key the key to delete
     * @return 1 if the key was deleted, 0 if it did not exist
     */
    Long del(String key);

    /**
     * Incrementally iterates hash fields and values.
     *
     * @param key the key
     * @param cursor the scan cursor
     * @return scan result with entries
     */
    ScanResult<Map.Entry<String, String>> hscan(String key, String cursor);

    /**
     * Incrementally iterates hash fields and values with scan parameters.
     *
     * @param key the key
     * @param cursor the scan cursor
     * @param params scan parameters
     * @return scan result with entries
     */
    ScanResult<Map.Entry<String, String>> hscan(String key, String cursor, ScanParams params);

    /**
     * Incrementally iterates set members.
     *
     * @param key the key
     * @param cursor the scan cursor
     * @return scan result with members
     */
    ScanResult<String> sscan(String key, String cursor);

    /**
     * Incrementally iterates keys matching a pattern.
     *
     * @param key the scan cursor or pattern
     * @param cursor the scan cursor
     * @param count the count hint
     * @return scan result with keys
     */
    ScanResult<String> scan(String key, String cursor, int count);

    /**
     * Incrementally iterates sorted set members with scores.
     *
     * @param key the key
     * @param cursor the scan cursor
     * @return scan result with tuples
     */
    ScanResult<Tuple> zscan(String key, String cursor);

    /**
     * Incrementally iterates set members with scan parameters.
     *
     * @param key the key
     * @param cursor the scan cursor
     * @param params scan parameters
     * @return scan result with members
     */
    ScanResult<String> sscan(String key, String cursor, ScanParams params);

    /**
     * Sets a byte array value for the given key.
     *
     * @param key the key
     * @param value the value
     * @return status code reply
     */
    String set(byte[] key, byte[] value);

    /**
     * Sets multiple byte array key-value pairs.
     *
     * @param keyvalues alternating keys and values
     */
    void mset(byte[]... keyvalues);

    /**
     * Gets the byte array value for the given key.
     *
     * @param key the key
     * @return the value, or null if the key does not exist
     */
    byte[] getBytes(byte[] key);

    /**
     * Gets the values of multiple keys.
     *
     * @param keys the keys
     * @return list of values
     */
    List<String> mget(String[] keys);

    /**
     * Gets the byte array values of multiple keys.
     *
     * @param keys the keys
     * @return list of byte array values
     */
    List<byte[]> mgetBytes(byte[]... keys);

    /**
     * Evaluates a cached Lua script by its SHA1 hash.
     *
     * @param sha1 the SHA1 hash of the script
     * @param keys the keys passed to the script
     * @param args the arguments passed to the script
     * @return the script result
     */
    Object evalsha(final String sha1, final List<String> keys, final List<String> args);

    /**
     * Evaluates a cached Lua script by its SHA1 hash using byte arrays.
     *
     * @param sha1 the SHA1 hash of the script
     * @param keys the keys passed to the script
     * @param args the arguments passed to the script
     * @return the script result
     */
    Object evalsha(byte[] sha1, List<byte[]> keys, List<byte[]> args);

    /**
     * Loads a Lua script into the script cache.
     *
     * @param script the script bytes
     * @param sampleKey a sample key for cluster slot routing
     * @return the SHA1 hash of the loaded script
     */
    byte[] scriptLoad(byte[] script, byte[] sampleKey);

    /**
     * Returns information about the Redis server.
     *
     * @param command the info section to retrieve
     * @return the server info string
     */
    String info(String command);

    /**
     * Waits for the specified number of replicas to acknowledge writes.
     *
     * @param key the key (used for cluster slot routing)
     * @param replicas the number of replicas to wait for
     * @param timeoutInMillis the timeout in milliseconds
     * @return the number of replicas that acknowledged
     */
    int waitReplicas(String key, int replicas, long timeoutInMillis);

    /**
     * Pings the Redis server.
     *
     * @return PONG if the server is running
     */
    String ping();
}
