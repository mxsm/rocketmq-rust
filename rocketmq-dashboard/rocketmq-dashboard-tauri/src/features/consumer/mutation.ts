import type { ConsumerGroupListItem, ConsumerMutationResult } from './types/consumer.types';

// The backend classifier is authoritative, including for manually entered names.
export const isReadOnlyConsumer = (consumer: ConsumerGroupListItem | null) => Boolean(consumer && (
    consumer.category === 'SYSTEM' || consumer.rawGroupName.trim().startsWith('%SYS%') || consumer.displayGroupName.trim().startsWith('%SYS%')
));
export const failedConsumerBrokers = (result: ConsumerMutationResult) =>
    result.targets.filter(target => target.kind === 'BROKER' && !target.success).map(target => target.target);
