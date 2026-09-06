
import { Kafka } from 'kafkajs';

const kafka_url = process.env.KAFKA_URL;

const kafka = new Kafka({ brokers: [kafka_url] });
const producer = kafka.producer();

await producer.connect();
await producer.send({
  topic: 'orders',
  messages: [
    { key: 'user-42', value: JSON.stringify({ orderId: 1, amount: 500 }) }
  ]
});

await producer.disconnect();