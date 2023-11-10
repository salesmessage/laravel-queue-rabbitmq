<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Connection;

use PhpAmqpLib\Channel\AbstractChannel;
use VladimirYuldashev\LaravelQueueRabbitMQ\Contracts\ChannelPool;

final class ChannelAsyncPool implements ChannelPool
{
    private $pool;

    public function __construct()
    {
        if (extension_loaded('swoole')) {
            $this->pool = new \Swoole\Coroutine\Channel();
        } elseif (extension_loaded('openswoole')) {
            $this->pool = new \OpenSwoole\Coroutine\Channel();
        } else {
            throw new \Exception('Async mode is not supported. Check if Swoole extension is installed');
        }
    }

    public function push(AbstractChannel $data): void
    {
        $this->pool->push($data);
    }

    public function pop(float $timeout = null)
    {
        return $this->pool->pop($timeout ?: -1);
    }

    public function isEmpty(): bool
    {
        return $this->pool->isEmpty();
    }
}
