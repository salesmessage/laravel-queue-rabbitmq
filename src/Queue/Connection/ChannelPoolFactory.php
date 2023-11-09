<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Connection;

use VladimirYuldashev\LaravelQueueRabbitMQ\Contracts\ChannelPool;

abstract class ChannelPoolFactory
{
    public static function make(bool $isAsync = false): ChannelPool
    {
        if ($isAsync) {
            return new ChannelAsyncPool();
        }
        return new ChannelSyncPool();
    }
}
