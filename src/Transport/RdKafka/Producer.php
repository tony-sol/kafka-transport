<?php

declare(strict_types=1);

namespace Transport\Transport\RdKafka;

use Transport\Contracts\MessageInterface;
use Transport\Contracts\SenderInterface;
use Transport\Exception\SendException;

/**
 * @property \RdKafka\Producer|null $actor
 * @method   \RdKafka\Producer getActor()
 */
class Producer extends AbstractTransport implements SenderInterface
{
    /**
     * Mutate actor config for producer
     *
     * @param \RdKafka\Conf $conf Kafka configuration.
     *
     * @return \RdKafka\Conf
     */
    protected function mutateActorConfig(\RdKafka\Conf $conf): \RdKafka\Conf
    {
        $conf->setDrMsgCb($this->getDrMsgCb());
        return $conf;
    }

    /**
     * Get or create Producer
     *
     * @param \RdKafka\Conf $conf Kafka configuration.
     *
     * @return \RdKafka\Producer
     */
    protected function createActor(\RdKafka\Conf $conf): \RdKafka\Producer
    {
        $producer = new \RdKafka\Producer($conf);
        register_shutdown_function(
            function (): void {
                while ($this->getActor()->getOutQLen() > 0) {
                    $this->getActor()->poll($this->config->getShutDownPollTimeoutMs());
                }
            }
        );
        return $producer;
    }

    /**
     * Send a message
     *
     * @param string                       $topicName The topic name.
     * @param MessageInterface|string|null $message   The message.
     * @param string                       $key       The topic key.
     *
     * @throws SendException
     *
     * @return void
     */
    public function send(
        string $topicName,
        MessageInterface|string|null $message,
        ?string $key = null,
    ): void {
        $this->process(
            $topicName,
            function (string $topicName) use ($message, $key): void {
                $this
                    ->getActor()
                    ->newTopic($topicName)
                    ->produce($this->config->getPartitionInd(), 0, (string)$message, $key)
                ;
                $this
                    ->getActor()
                    ->poll($this->config->getPollTimeoutMs())
                ;
            },
            function (\Throwable $throwable) use ($topicName, $key): void {
                throw new SendException("Error producing to topic {$topicName}:{$key}", $throwable);
            }
        );
    }

    /**
     * Get delivery report callback
     *
     * @return \Closure
     */
    private function getDrMsgCb(): \Closure
    {
        /**
         * @psalm-suppress UnusedClosureParam
         * phpcs:disable Squiz.NamingConventions.ValidVariableName.MemberNotCamelCaps
         */
        return function (\RdKafka\Producer $producer, \RdKafka\Message $message): void {
            $context = [
                'topicName'  => $message->topic_name,
                'messageErr' => $message->err,
            ];
            switch ($message->err) {
                case \RD_KAFKA_RESP_ERR_NO_ERROR:
                    return;
                case \RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
                    $this->logger->warning(
                        "ErrorSendToKafkaUnknownTopic_{$message->topic_name}",
                        $context
                    );
                    break;
                case \RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:
                    $this->logger->warning(
                        "ErrorSendToKafkaMessageTimedOutTopic_{$message->topic_name}",
                        $context
                    );
                    break;
                default:
                    $this->logger->warning(
                        "ErrorSendToKafkaTopic_{$message->topic_name}",
                        $context
                    );
            }
        };
        // phpcs:enable
    }
}
