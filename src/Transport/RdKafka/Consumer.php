<?php

declare(strict_types=1);

namespace Transport\Transport\RdKafka;

use Transport\Contracts\MessageInterface;
use Transport\Contracts\ReceiverInterface;
use Transport\Exception\ReturnException;

/**
 * @property \RdKafka\KafkaConsumer|null $actor
 * @method   \RdKafka\KafkaConsumer getActor()
 */
class Consumer extends AbstractTransport implements ReceiverInterface
{
    /** @var array<string, bool> */
    protected array $subscriptions = [];

    /**
     * Mutate actor config for consumer
     *
     * @param \RdKafka\Conf $conf Kafka configuration.
     *
     * @return \RdKafka\Conf
     */
    protected function mutateActorConfig(\RdKafka\Conf $conf): \RdKafka\Conf
    {
        $conf->set('auto.offset.reset', $this->config->getAutoOffsetReset());
        /**
         * @note temporary disabled due to fix its hanging
         * $conf->setRebalanceCb($this->getRebalanceCb());
         */
        return $conf;
    }

    /**
     * Get or create KafkaConsumer
     *
     * @param \RdKafka\Conf $conf Kafka configuration.
     *
     * @return \RdKafka\KafkaConsumer
     */
    protected function createActor(\RdKafka\Conf $conf): \RdKafka\KafkaConsumer
    {
        $consumer = new \RdKafka\KafkaConsumer($conf);
        register_shutdown_function(
            function (): void {
                $this->actor?->close();
            }
        );
        return $consumer;
    }

    /**
     * Receive a message
     *
     * @param string $topicName The topic name.
     *
     * @throws ReturnException
     *
     * @return MessageInterface|string|null
     *
     * @psalm-suppress MixedReturnStatement
     * @psalm-suppress MixedInferredReturnType
     */
    public function return(string $topicName): MessageInterface|string|null
    {
        return $this->process(
            $topicName,
            function (string $topicName): string {
                $this->subscribe($topicName);
                $message = $this->getActor()->consume($this->config->getConsumeTimeoutMs());
                return $this->processMessage($message)->payload;
            },
            static function (\Throwable $throwable) use ($topicName): void {
                throw new ReturnException("Error consuming from topic {$topicName}", $throwable);
            }
        );
    }

    /**
     * Subscribe to a topic
     *
     * @param string $topicName The topic name.
     *
     * @throws \RdKafka\Exception
     * @throws \InvalidArgumentException
     *
     * @return void
     */
    private function subscribe(string $topicName): void
    {
        if (($this->subscriptions[$topicName] ?? false) === false) {
            $this->subscriptions[$topicName] = true;
            $this->getActor()->subscribe([$topicName]);
        }
    }

    /**
     * Process a received message
     *
     * @param \RdKafka\Message $message Kafka message.
     *
     * @throws \RdKafka\Exception
     * @throws \RuntimeException
     * @throws \InvalidArgumentException
     *
     * @return \RdKafka\Message
     */
    private function processMessage(\RdKafka\Message $message): \RdKafka\Message
    {
        switch ($message->err) {
            case \RD_KAFKA_RESP_ERR_NO_ERROR:
                $this->getActor()->commitAsync($message);
                return $message;
            case \RD_KAFKA_RESP_ERR__PARTITION_EOF:
            case \RD_KAFKA_RESP_ERR__TIMED_OUT:
                $this->logger->warning($message->errstr());
                return $message;
            default:
                throw new \RuntimeException($message->errstr(), $message->err);
        }
    }

    /**
     * Get rebalance callback
     *
     * @phpstan-ignore-next-line
     *
     * @return \Closure
     */
    private function getRebalanceCb(): \Closure
    {
        return function (\RdKafka\KafkaConsumer $kafka, int $error, array $partitions = null): void {
            $context = [
                'consumer'   => $kafka,
                'partitions' => $partitions,
            ];
            switch ($error) {
                case \RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    $this->logger->notice('KafkaRebalanceAssignPartitions', $context);
                    /** @var array<array-key, \RdKafka\TopicPartition>|null $partitions */
                    $kafka->assign($partitions);
                    break;
                case \RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    $this->logger->notice('KafkaRebalanceRevokePartitions', $context);
                    $kafka->assign(null);
                    break;
                default:
                    $message = rd_kafka_err2str($error);
                    $context['error_code'] = $error;
                    $this->logger->error("Unhandled rebalance error: {$message}", $context);
                    break;
            }
        };
    }
}
