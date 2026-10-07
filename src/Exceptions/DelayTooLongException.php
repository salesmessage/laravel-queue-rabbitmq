<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Exceptions;

final class DelayTooLongException extends \RuntimeException
{
    public static function forQueue(string $queue, int $delaySeconds, int $maxDelaySeconds): self
    {
        return new self(sprintf(
            'Delay of %d seconds for queue "%s" exceeds the maximum of %d seconds.',
            $delaySeconds,
            $queue,
            $maxDelaySeconds
        ));
    }
}
