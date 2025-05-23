import assert from 'node:assert';
import test from 'node:test';
import { EventBroker } from '../dist/broker.js';

test('EventBroker partitions topics and advances consumer group offsets', () => {
  const broker = new EventBroker();
  broker.createTopic('orders', 2);

  broker.publish('orders', { id: 101, amount: 50 }, 'user_a');
  broker.publish('orders', { id: 102, amount: 80 }, 'user_a');

  const batch1 = broker.consume('billing-service', 'orders', 1, 10);
  assert.ok(batch1.length > 0);

  const batch2 = broker.consume('billing-service', 'orders', 1, 10);
  assert.strictEqual(batch2.length, 0); // No new unconsumed messages
});
