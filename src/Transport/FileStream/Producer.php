<?php

declare(strict_types=1);

namespace Transport\Transport\FileStream;

use Transport\Contracts\MessageInterface;
use Transport\Contracts\SenderInterface;
use Transport\Exception\SendException;

/**
 * @note usage
 * single message:
 *      (new Producer())->send($topic, $message, $key);
 * in sending in loop create Producer instance before loop
 *      $producer = new Producer(); loop() {$producer->send($topic, $message, $key)};
 */
class Producer extends AbstractTransport implements SenderInterface
{
    /**
     * Send a message to topic
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
        string $key = null,
    ): void {
        $resource = $this->getResource('ab', $topicName);
        /** @psalm-suppress MissingClosureParamType */
        $producer = function ($resource) use ($topicName): \Generator {
            /** @phpstan-ignore-next-line */
            while (true) {
                try {
                    /**
                     * @var string $message
                     * @var resource $resource
                     */
                    $message = yield;
                    fwrite($resource, $message . PHP_EOL);
                } catch (\Throwable $throwable) {
                    $this->logger->error(
                        $throwable->getMessage(),
                        ['topicName' => $topicName]
                    );
                    throw new SendException('Error occurred while sending message', $throwable);
                }
            }
        };
        $producer($resource)->send((string)$message);
    }
}
