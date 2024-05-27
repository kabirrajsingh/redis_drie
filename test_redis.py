import redis
import redis.client

client=redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

client.flushdb()
# client.set("key1","value1")
# client.set("key2","value2")
# client.set("key3","value3")
# client.hset("hkey1",mapping={
#   "_k1":"_v1",
#   "_k2":"_v2",
#   "_k3":"_v3",
# })

print(client.smembers("1"))

# allKeys=client.keys()
# for key in allKeys:
#   if key.startswith("h"):
#     print(key,client.hgetall(key))
#   else:
#     print(key,client.get(key))