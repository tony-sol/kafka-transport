<?php

declare(strict_types=1);

namespace Transport\Transport\RdKafka;

class Config
{
    public const SECURITY_PROTOCOL_PLAINTEXT = 'plaintext';
    public const SECURITY_PROTOCOL_SSL       = 'ssl';
    public const SECURITY_PROTOCOL_SASL_SSL  = 'sasl_ssl';

    public const AUTO_OFFSET_RESET_NONE     = 'none';
    public const AUTO_OFFSET_RESET_EARLIEST = 'earliest';
    public const AUTO_OFFSET_RESET_LATEST   = 'latest';

    protected const ACKS_NO     = 0;
    protected const ACKS_LEADER = 1;
    protected const ACKS_ALL    = -1;

    /**
     * Config constructor
     *
     * @param string|null  $clientId                  Client ID.
     * @param string|null  $groupId                   Group ID.
     * @param string|null  $brokers                   Commaseparated brokers list.
     * @param string|null  $securityProtocol          Security protocol ssl|sasl_ssl|plaintext.
     * @param string|null  $userName                  User name.
     * @param string|null  $password                  User password.
     * @param string|null  $sslCaLocation             SSL CA location.
     * @param string|null  $sslCertificateLocation    SSL certificate location.
     * @param string|null  $sslKeyLocation            SSL key location.
     * @param integer|null $queueBufferingMaxMs       Queue buffering max ms.
     * @param integer|null $queueBufferingMaxMessages Queue buffering max messages.
     * @param integer|null $messageSendMaxRetries     Message send max retries.
     * @param integer|null $acks                      Acks.
     * @param integer|null $requestTimeoutMs          Request timeout ms.
     * @param integer|null $messageTimeoutMs          Message timeout ms.
     * @param integer|null $pollTimeoutMs             Polling timeout ms.
     * @param integer|null $consumeTimeoutMs          Consume timeout ms.
     * @param integer|null $shutDownPollTimeoutMs     Shutdown polling timeout ms.
     * @param integer|null $socketTimeoutMs           Socket timeout ms.
     * @param integer|null $metaDataTimeoutMs         Metadata timeout ms.
     * @param string[]     $ignoredTopics             Ignored topics list.
     * @param integer|null $partitionInd              Partition index.
     * @param string|null  $autoOffsetReset           Auto offset reset policy.
     * @param boolean      $enableIdempotence         Whether to enable idempotence.
     */
    public function __construct(
        protected ?string $clientId = null,
        protected ?string $groupId = null,
        protected ?string $brokers = null,
        protected ?string $securityProtocol = null,
        protected ?string $userName = null,
        protected ?string $password = null,
        protected ?string $sslCaLocation = null,
        protected ?string $sslCertificateLocation = null,
        protected ?string $sslKeyLocation = null,
        protected ?int $queueBufferingMaxMs = null,
        protected ?int $queueBufferingMaxMessages = null,
        protected ?int $messageSendMaxRetries = null,
        protected ?int $acks = null,
        protected ?int $requestTimeoutMs = null,
        protected ?int $messageTimeoutMs = null,
        protected ?int $pollTimeoutMs = null,
        protected ?int $consumeTimeoutMs = null,
        protected ?int $shutDownPollTimeoutMs = null,
        protected ?int $socketTimeoutMs = null,
        protected ?int $metaDataTimeoutMs = null,
        protected ?array $ignoredTopics = null,
        protected ?int $partitionInd = null,
        protected ?string $autoOffsetReset = null,
        protected ?bool $enableIdempotence = null,
    ) {
        /** @todo refactor default configs */
    }

    /**
     * Get clientId from configuration or ENV
     *
     * @throws \InvalidArgumentException
     *
     * @return string
     */
    public function getClientId(): string
    {
        return $this->clientId
            ?: getenv('KAFKA_CLIENT_ID')
            ?: throw new \InvalidArgumentException('Client ID is not set');
    }

    /**
     * Get groupId from configuration or ENV
     *
     * @throws \InvalidArgumentException
     *
     * @return string
     */
    public function getGroupId(): string
    {
        return $this->groupId
            ?: getenv('KAFKA_GROUP_ID')
            ?: throw new \InvalidArgumentException('Group ID is not set');
    }

    /**
     * Get brokers from configuration or ENV
     *
     * @throws \InvalidArgumentException
     *
     * @return string
     */
    public function getBrokers(): string
    {
        return $this->brokers
            ?: getenv('KAFKA_BROKER_LIST')
            ?: throw new \InvalidArgumentException('Broker list is not set');
    }

    /**
     * Get security protocol from configuration or 'plaintext' as default
     *
     * @return string
     */
    public function getSecurityProtocol(): string
    {
        return (string)($this->securityProtocol ?: static::SECURITY_PROTOCOL_PLAINTEXT);
    }

    /**
     * Get user name from configuration
     *
     * @return string
     */
    public function getUserName(): string
    {
        return (string)$this->userName;
    }

    /**
     * Get user password from configuration
     *
     * @return string
     */
    public function getPassword(): string
    {
        return (string)$this->password;
    }

    /**
     * Get SSL CA location from configuration
     *
     * @return string
     */
    public function getSslCaLocation(): string
    {
        return (string)$this->sslCaLocation;
    }

    /**
     * Get SSL certificate location from configuration
     *
     * @return string
     */
    public function getSslCertificateLocation(): string
    {
        return (string)$this->sslCertificateLocation;
    }

    /**
     * Get SSL key location from configuration
     *
     * @return string
     */
    public function getSslKeyLocation(): string
    {
        return (string)$this->sslKeyLocation;
    }

    /**
     * Get queue buffering max ms from configuration or 3000 as default
     *
     * @return integer
     */
    public function getQueueBufferingMaxMs(): int
    {
        return $this->queueBufferingMaxMs ?: 3000;
    }

    /**
     * Get queue buffering max messages from configuration or 50000 as default
     *
     * @return integer
     */
    public function getQueueBufferingMaxMessages(): int
    {
        return $this->queueBufferingMaxMessages ?: 50000;
    }

    /**
     * Get message send max retries from configuration or 3 as default
     *
     * @return integer
     */
    public function getMessageSendMaxRetries(): int
    {
        return $this->messageSendMaxRetries ?: 3;
    }

    /**
     * Get acks from configuration or ACKS_LEADER as default
     *
     * @return integer
     */
    public function getAcks(): int
    {
        return (int)($this->acks ?? static::ACKS_LEADER);
    }

    /**
     * Get request timeout ms from configuration or 5000 as default
     *
     * @return integer
     */
    public function getRequestTimeoutMs(): int
    {
        return $this->requestTimeoutMs ?: 5000;
    }

    /**
     * Get message timeout ms from configuration or 5000 as default
     *
     * @return integer
     */
    public function getMessageTimeoutMs(): int
    {
        return $this->messageTimeoutMs ?: 5000;
    }

    /**
     * Get poll timeout ms from configuration or 1000 as default
     *
     * @return integer
     */
    public function getPollTimeoutMs(): int
    {
        return $this->pollTimeoutMs ?: 1000;
    }

    /**
     * Get consume timeout ms from configuration or 20000 as default
     *
     * @return integer
     */
    public function getConsumeTimeoutMs(): int
    {
        return $this->consumeTimeoutMs ?: 20000;
    }

    /**
     * Get shutdown poll timeout ms from configuration or 10000 as default
     *
     * @return integer
     */
    public function getShutDownPollTimeoutMs(): int
    {
        return $this->shutDownPollTimeoutMs ?: 10000;
    }

    /**
     * Get socket timeout ms from configuration or 3000 as default
     *
     * @return integer
     */
    public function getSocketTimeoutMs(): int
    {
        return $this->socketTimeoutMs ?: 3000;
    }

    /**
     * Get metadata timeout ms from configuration or 3000 as default
     *
     * @return integer
     */
    public function getMetaDataTimeoutMs(): int
    {
        return $this->metaDataTimeoutMs ?: 3000;
    }

    /**
     * Get ignored topics list
     *
     * @return string[]
     */
    public function getIgnoredTopics(): array
    {
        return $this->ignoredTopics ?: [];
    }

    /**
     * Get partition index from configuration or \RD_KAFKA_PARTITION_UA as default
     *
     * @return integer
     */
    public function getPartitionInd(): int
    {
        return ($this->partitionInd ?? \RD_KAFKA_PARTITION_UA);
    }

    /**
     * Get auto offset reset from configuration or 'earliest' as default
     *
     * @return string
     */
    public function getAutoOffsetReset(): string
    {
        return (string)($this->autoOffsetReset ?: static::AUTO_OFFSET_RESET_EARLIEST);
    }

    /**
     * Get enable idempotence from configuration or false as default
     *
     * @return boolean
     */
    public function isEnableIdempotence(): bool
    {
        return $this->enableIdempotence ?: false;
    }
}
