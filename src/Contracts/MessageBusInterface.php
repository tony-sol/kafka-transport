<?php

declare(strict_types=1);

namespace Transport\Contracts;

interface MessageBusInterface extends SenderInterface, ReceiverInterface
{
}
