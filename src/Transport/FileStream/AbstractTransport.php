<?php

declare(strict_types=1);

namespace Transport\Transport\FileStream;

use Psr\Log\LoggerInterface;

abstract class AbstractTransport
{
    /** @var array<array-key, resource> */
    protected static array $resourceContainer = [];

    protected string $storagePath;

    /**
     * AbstractTransport constructor
     *
     * @param LoggerInterface $logger Logger instance.
     */
    public function __construct(
        protected LoggerInterface $logger
    ) {
        $this->storagePath = (getenv('MESSENGER_STORAGE') ?: '/tmp');
    }

    /**
     * Get or create resource by topicName and key with mode
     *
     * @param string $mode      Access mode of resource opening.
     * @param string $topicName The topic name.
     *
     * @throws \RuntimeException
     *
     * @return resource
     */
    protected function getResource(string $mode, string $topicName)
    {
        /** @todo store resources in APC cache if possible */
        $handler = (static::$resourceContainer[$topicName] ?? null);
        if ($handler === null) {
            $handler = fopen("{$this->storagePath}/{$topicName}", $mode);
            if (\is_resource($handler) === false) {
                throw new \RuntimeException("Failed to open file {$topicName}");
            }
            static::$resourceContainer[$topicName] = $handler;
            register_shutdown_function(
                static function () use ($topicName) {
                    fclose(static::$resourceContainer[$topicName]);
                    unset(static::$resourceContainer[$topicName]);
                }
            );
        }
        return static::$resourceContainer[$topicName];
    }
}
