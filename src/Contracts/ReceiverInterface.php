<?php

declare(strict_types=1);

namespace Transport\Contracts;

use Transport\Exception\BaseException;

interface ReceiverInterface
{
    /**
     * Receive a message
     *
     * @param string $topicName The topic name.
     *
     * @throws BaseException
     *
     * @return MessageInterface|string|null
     */
    public function return(string $topicName): MessageInterface|string|null;
}
