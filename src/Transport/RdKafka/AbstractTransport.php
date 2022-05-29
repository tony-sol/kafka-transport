<?php

declare(strict_types=1);

namespace Transport\Transport\RdKafka;

use Psr\Log\LoggerInterface;

abstract class AbstractTransport
{
    protected \RdKafka\Producer|\RdKafka\KafkaConsumer|null $actor = null;

    /**
     * AbstractTransport constructor
     *
     * @param Config          $config RdKafka config.
     * @param LoggerInterface $logger Logger instance.
     */
    public function __construct(
        protected Config $config,
        protected LoggerInterface $logger
    ) {
    }

    /**
     * Mutate actor config for producer or consumer
     *
     * @param \RdKafka\Conf $conf Kafka configuration.
     *
     * @return \RdKafka\Conf
     */
    abstract protected function mutateActorConfig(\RdKafka\Conf $conf): \RdKafka\Conf;

    /**
     * Get or create actor
     *
     * @param \RdKafka\Conf $conf Kafka configuration.
     *
     * @return \RdKafka\Producer|\RdKafka\KafkaConsumer
     */
    abstract protected function createActor(\RdKafka\Conf $conf): \RdKafka\KafkaConsumer|\RdKafka\Producer;

    /**
     * Get actor's metadata
     *
     * @throws \RdKafka\Exception
     * @throws \InvalidArgumentException
     *
     * @return \RdKafka\Metadata
     */
    public function getMetadata(): \RdKafka\Metadata
    {
        /** @psalm-suppress NullArgument */
        return $this->getActor()->getMetadata(
            true,
            null,
            $this->config->getMetaDataTimeoutMs()
        );
    }

    /**
     * Get actor's metadata for given topic
     *
     * @param string $topicName Topic name.
     *
     * @throws \RdKafka\Exception
     * @throws \InvalidArgumentException
     *
     * @return \RdKafka\Metadata
     */
    public function getMetadataByTopic(string $topicName): \RdKafka\Metadata
    {
        /** @psalm-suppress ArgumentTypeCoercion */
        return $this->getActor()->getMetadata(
            false,
            $this->getActor()->newTopic($topicName),
            $this->config->getMetaDataTimeoutMs()
        );
    }

    /**
     * Get or create actor
     *
     * @throws \InvalidArgumentException
     *
     * @return \RdKafka\Producer|\RdKafka\KafkaConsumer
     */
    protected function getActor(): \RdKafka\KafkaConsumer|\RdKafka\Producer
    {
        if ($this->actor === null) {
            $this->actor = $this->createActor(
                $this->mutateActorConfig(
                    $this->createRdKafkaActorConfig()
                )
            );
        }
        return $this->actor;
    }

    /**
     * Handle given topic
     *
     * @param string   $topicName        The topic name.
     * @param \Closure $handler          The topic handler.
     * @param \Closure $exceptionHandler The exception handler.
     *
     * @return mixed
     */
    protected function process(string $topicName, \Closure $handler, \Closure $exceptionHandler): mixed
    {
        if (\in_array($topicName, $this->config->getIgnoredTopics(), true)) {
            $this->logger->info("Topic {$topicName} is ignored");
            return null;
        }
        try {
            return $handler($topicName);
        } catch (\Throwable $throwable) {
            $this->logger->error(
                'Kafka got error',
                [
                    'errorMessage' => $throwable->getMessage(),
                    'topicName'    => $topicName,
                ]
            );
            $exceptionHandler($throwable);
            return null;
        }
    }

    /**
     * Create common configuration for Kafka
     *
     * @throws \InvalidArgumentException
     *
     * @return \RdKafka\Conf
     */
    protected function createRdKafkaActorConfig(): \RdKafka\Conf
    {
        $conf = new \RdKafka\Conf();
        $conf->set('client.id', $this->config->getClientId());
        $conf->set('group.id', $this->config->getGroupId());
        $conf->set('metadata.broker.list', $this->config->getBrokers());

        $securityProtocol = $this->config->getSecurityProtocol();
        $conf->set('security.protocol', $securityProtocol);

        switch ($securityProtocol) {
            case Config::SECURITY_PROTOCOL_SSL:
                $this->setSslConfig($conf);
                break;
            case Config::SECURITY_PROTOCOL_PLAINTEXT:
                $this->setPlaintextConfig($conf);
                break;
            case Config::SECURITY_PROTOCOL_SASL_SSL:
                $this->setSaslSslConfig($conf);
                break;
            default:
                throw new \InvalidArgumentException("Not supported security protocol: {$securityProtocol}");
        }

        $conf->set('api.version.request', 'false');

        if ($this->config->isEnableIdempotence()) {
            $conf->set('enable.idempotence', 'true');
        }

        $conf->set('socket.timeout.ms', (string)$this->config->getSocketTimeoutMs());

        $queueBufferingMaxMs = $this->config->getQueueBufferingMaxMs();
        $messageTimeoutMs    = $this->config->getMessageTimeoutMs();

        if ($queueBufferingMaxMs > $messageTimeoutMs) {
            // phpcs:disable Generic.Strings.UnnecessaryStringConcat.Found
            $this->logger->warning(
                'queue.buffering.max.ms greater than message.timeout.ms. ' .
                'For queue.buffering.max.ms the minimum value was selected.',
                [
                    'queue.buffering.max.ms' => $queueBufferingMaxMs,
                    'message.timeout.ms'     => $messageTimeoutMs,
                ]
            );
            // phpcs:enable
            $queueBufferingMaxMs = $messageTimeoutMs;
        }

        if (\function_exists('pcntl_sigprocmask') && pcntl_sigprocmask(SIG_BLOCK, [SIGIO])) {
            $conf->set('internal.termination.signal', (string)SIGIO);
        } else {
            $conf->set('queue.buffering.max.ms', (string)$queueBufferingMaxMs);
        }

        $conf->set('queue.buffering.max.messages', (string)$this->config->getQueueBufferingMaxMessages());

        $messageSendMaxRetries = $this->config->getMessageSendMaxRetries();
        if ($messageSendMaxRetries > 0) {
            $conf->set('message.send.max.retries', (string)$messageSendMaxRetries);
        }

        $conf->set('request.required.acks', (string)$this->config->getAcks());
        $conf->set('request.timeout.ms', (string)$this->config->getRequestTimeoutMs());
        $conf->set('message.timeout.ms', (string)$this->config->getMessageTimeoutMs());

        $conf->setErrorCb($this->getErrorCb());
        $conf->setStatsCb($this->getStatsCb());

        return $conf;
    }

    /**
     * Get error handler
     *
     * @return \Closure
     */
    protected function getErrorCb(): \Closure
    {
        /** @psalm-suppress UnusedClosureParam */
        return function (\RdKafka $kafka, int $error, string $reason): void {
            $errorMessage = \rd_kafka_err2str($error);
            $message      = "Kafka error: {$errorMessage} (reason: {$reason})";
            $this->logger->error($message);
            throw new \RuntimeException($message, $error);
        };
    }

    /**
     * Get stats handler
     *
     * @return \Closure
     */
    protected function getStatsCb(): \Closure
    {
        /** @psalm-suppress UnusedClosureParam */
        return function (\RdKafka $kafka, string $json, int $jsonLen): void {
            /** @todo make passing stat processor from outside */
        };
    }

    /**
     * Set SSL config for kafka connection
     *
     * @param \RdKafka\Conf $conf Kafka configuration.
     *
     * @return void
     */
    protected function setSslConfig(\RdKafka\Conf $conf): void
    {
        $conf->set('ssl.ca.location', $this->config->getSslCaLocation());
        $conf->set('ssl.certificate.location', $this->config->getSslCertificateLocation());
        $conf->set('ssl.key.location', $this->config->getSslKeyLocation());
    }

    /**
     * Set plaintext config for kafka connection
     *
     * @param \RdKafka\Conf $conf Kafka configuration.
     *
     * @return void
     */
    protected function setPlaintextConfig(\RdKafka\Conf $conf): void
    {
        $conf->set('sasl.username', $this->config->getUserName());
        $conf->set('sasl.password', $this->config->getPassword());
    }

    /**
     * Set SASL/SSL config for kafka connection
     *
     * @param \RdKafka\Conf $conf Kafka configuration.
     *
     * @return void
     */
    protected function setSaslSslConfig(\RdKafka\Conf $conf): void
    {
        $this->setSslConfig($conf);
        $conf->set('sasl.mechanism', 'PLAIN');
        $this->setPlaintextConfig($conf);
    }
}
