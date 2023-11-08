<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Contracts;

use PhpAmqpLib\Channel\AbstractChannel;

interface ChannelPool
{
    public function push(AbstractChannel $data): void;

    public function pop(float $timeout = null): ?AbstractChannel;

    public function isEmpty(): bool;
}
