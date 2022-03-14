from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigSource
from confluent_kafka import KafkaException
import sys
import threading
import logging

#def list_topic()

broker = "104.199.158.20:9092"

# Create Admin client
a = AdminClient({'bootstrap.servers': broker})
md = a.list_topics(timeout=10)
print("Cluster {} metadata (response from broker {}):".format(md.cluster_id, md.orig_broker_name))

print(" {} topics:".format(len(md.topics)))
for t in iter(md.topics.values()):
        if t.error is not None:
                errstr = ": {}".format(t.error)
        else:
                errstr = ""

        print("  \"{}\" with {} partition(s){}".format(t, len(t.partitions), errstr))

        for p in iter(t.partitions.values()):
                if p.error is not None:
                        errstr = ": {}".format(p.error)
                else:
                        errstr = ""

                print("partition {} leader: {}, replicas: {},"
                      " isrs: {} errstr: {}".format(p.id, p.leader, p.replicas,
                                                    p.isrs, errstr))
