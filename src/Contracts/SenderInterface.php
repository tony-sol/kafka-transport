<?php

declare(strict_types=1);

namespace Transport\Contracts;

use Transport\Exception\BaseException;

interface SenderInterface
{
    /**
     * Send a message
     *
     * @param string                       $topicName The topic name.
     * @param MessageInterface|string|null $message   The message.
     * @param string                       $key       The topic key.
     *
     * @throws BaseException
     *
     * @return void
     */
    public function send(
        string $topicName,
        MessageInterface|string|null $message,
        string $key = null,
    ): void;
}
