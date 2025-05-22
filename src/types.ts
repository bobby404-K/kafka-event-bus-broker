export interface TopicMessage<T = any> {
  offset: number;
  timestamp: number;
  key?: string;
  value: T;
}

export interface PartitionStats {
  topic: string;
  partitionId: number;
  totalMessages: number;
  earliestOffset: number;
  latestOffset: number;
}
