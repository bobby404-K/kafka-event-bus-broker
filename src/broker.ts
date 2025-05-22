import { TopicMessage, PartitionStats } from './types.js';

export class EventBroker {
  private topics = new Map<string, Map<number, TopicMessage[]>>();
  private consumerOffsets = new Map<string, Map<string, number>>(); // group -> topic:partition -> offset

  createTopic(topic: string, partitions = 3): void {
    if (!this.topics.has(topic)) {
      const partMap = new Map<number, TopicMessage[]>();
      for (let i = 0; i < partitions; i++) partMap.set(i, []);
      this.topics.set(topic, partMap);
    }
  }

  publish<T>(topic: string, value: T, key?: string): { partition: number; offset: number } {
    const partMap = this.topics.get(topic);
    if (!partMap) throw new Error(`Topic ${topic} does not exist`);

    const partCount = partMap.size;
    const partition = key ? Math.abs(this.hashString(key)) % partCount : Math.floor(Math.random() * partCount);
    const log = partMap.get(partition)!;

    const offset = log.length;
    log.push({ offset, timestamp: Date.now(), key, value });
    return { partition, offset };
  }

  consume<T>(group: string, topic: string, partition: number, maxMessages = 10): TopicMessage<T>[] {
    const partMap = this.topics.get(topic);
    if (!partMap) return [];
    const log = partMap.get(partition);
    if (!log) return [];

    const groupKey = `${topic}:${partition}`;
    let groupMap = this.consumerOffsets.get(group);
    if (!groupMap) {
      groupMap = new Map();
      this.consumerOffsets.set(group, groupMap);
    }

    const currentOffset = groupMap.get(groupKey) || 0;
    const slice = log.slice(currentOffset, currentOffset + maxMessages);
    if (slice.length > 0) {
      groupMap.set(groupKey, currentOffset + slice.length);
    }
    return slice;
  }

  private hashString(s: string): number {
    let hash = 0;
    for (let i = 0; i < s.length; i++) hash = (hash << 5) - hash + s.charCodeAt(i);
    return hash;
  }
}
