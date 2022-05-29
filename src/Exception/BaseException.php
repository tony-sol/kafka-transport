<?php

declare(strict_types=1);

namespace Transport\Exception;

class BaseException extends \RuntimeException
{
    /**
     * Exceptions constructor
     *
     * @param string          $message  Exception message.
     * @param \Throwable|null $previous Previous exception.
     * @param integer         $code     Exception code.
     */
    public function __construct(string $message = '', ?\Throwable $previous = null, int $code = 0)
    {
        parent::__construct($message, $code, $previous);
    }
}
