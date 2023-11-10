<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Contracts;

use PhpAmqpLib\Channel\AbstractChannel;

interface ChannelPool
{
    public function push(AbstractChannel $data): void;

    /**
     * @param float|null $timeout
     * @return AbstractChannel|null|bool
     */
    public function pop(?float $timeout = null);

    public function isEmpty(): bool;
}
