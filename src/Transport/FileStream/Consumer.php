<?php

declare(strict_types=1);

namespace Transport\Transport\FileStream;

use Transport\Contracts\MessageInterface;
use Transport\Contracts\ReceiverInterface;
use Transport\Exception\ReturnException;

/**
 * @note usage
 * single message:
 *      (new Consumer())->return($topic);
 * in sending in loop create Consumer instance before loop
 *      $consumer = new Consumer(); loop() {$message = $consumer->return($topic)};
 */
class Consumer extends AbstractTransport implements ReceiverInterface
{
    /**
     * Return a message from topic
     *
     * @param string $topicName Topic name.
     *
     * @throws ReturnException
     *
     * @return MessageInterface|string|null
     */
    public function return(string $topicName): MessageInterface|string|null
    {
        $resource = $this->getResource('rb', $topicName);
        /** @psalm-suppress MissingClosureParamType */
        $consumer = function ($resource) use ($topicName): \Generator {
            while (true) {
                try {
                    /** @var resource $resource */
                    $message = stream_get_line($resource, 0, PHP_EOL);
                    if ($message !== false) {
                        yield $message;
                    }
                } catch (\Throwable $throwable) {
                    $this->logger->error(
                        $throwable->getMessage(),
                        ['topicName' => $topicName]
                    );
                    throw new ReturnException('Error occurred while returning message', $throwable);
                }
            }
        };
        return $consumer($resource)->current() ?: null;
    }
}
