<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Connection;

use PhpAmqpLib\Channel\AbstractChannel;
use VladimirYuldashev\LaravelQueueRabbitMQ\Contracts\ChannelPool;

/**
 * For sync processing we don't need stack and just use class property everytime. since coroutines are not supported.
 */
final class ChannelSyncPool implements ChannelPool
{
    private ?AbstractChannel $channel = null;

    public function push(AbstractChannel $data): void
    {
        if (!$this->channel) {
            $this->channel = $data;
        }
    }

    public function pop(float $timeout = null): ?AbstractChannel
    {
        return $this->channel;
    }

    public function isEmpty(): bool
    {
        return false;
    }
}
