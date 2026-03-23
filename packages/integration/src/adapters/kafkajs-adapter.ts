// ---------------------------------------------------------------------------
// kafkajs adapter — wraps kafkajs into KafkaClient interface
// ---------------------------------------------------------------------------

import { Kafka } from "kafkajs";
import type { KafkaClient, KafkaConsumer, KafkaProducer, KafkaAdmin } from "@promin/kafka";

export function createKafkajsClient(broker: string): KafkaClient {
  const kafka = new Kafka({ brokers: [broker], logLevel: 0 });

  return {
    producer(): KafkaProducer {
      const p = kafka.producer();
      return {
        connect: () => p.connect(),
        disconnect: () => p.disconnect(),
        send: (params) => p.send(params) as unknown as Promise<void>,
      };
    },

    consumer(config): KafkaConsumer {
      const c = kafka.consumer({ groupId: config.groupId });
      return {
        connect: () => c.connect(),
        disconnect: () => c.disconnect(),
        subscribe: (params) =>
          c.subscribe({ topic: params.topic, fromBeginning: params.fromBeginning }),
        run: (params) =>
          c.run({
            autoCommit: params.autoCommit,
            eachMessage: params.eachMessage
              ? (payload) =>
                  params.eachMessage!({
                    topic: payload.topic,
                    partition: payload.partition,
                    message: {
                      key: payload.message.key,
                      value: payload.message.value,
                      offset: payload.message.offset,
                      timestamp: payload.message.timestamp ?? "",
                    },
                  })
              : undefined,
          }),
        commitOffsets: (offsets) => c.commitOffsets(offsets),
        seek: (params) => c.seek(params),
      };
    },

    admin(): KafkaAdmin {
      const a = kafka.admin();
      return {
        connect: () => a.connect(),
        disconnect: () => a.disconnect(),
        fetchOffsets: (params) => a.fetchOffsets(params),
        fetchTopicOffsetsByTimestamp: async (topic, timestamp) => {
          const offsets = await a.fetchTopicOffsetsByTimestamp(topic, timestamp);
          return offsets.map((o) => ({ partition: o.partition, offset: o.offset }));
        },
      };
    },
  };
}

export async function createTopic(broker: string, topic: string, partitions = 1) {
  const kafka = new Kafka({ brokers: [broker], logLevel: 0 });
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
    waitForLeaders: true,
  });
  await admin.disconnect();
}
