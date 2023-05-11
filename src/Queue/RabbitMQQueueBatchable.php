<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue;

use PhpAmqpLib\Connection\AbstractConnection;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue as BaseRabbitMQQueue;

class RabbitMQQueueBatchable extends BaseRabbitMQQueue
{
    public function push($job, $data = '', $queue = null)
    {
        $queue = $queue ?: $job->onQueue();
        return parent::push($job, $data, $queue);
    }


    public function pushRaw($payload, $queue = null, array $options = []): int|string|null
    {
        $options['exchange'] = 'ex.' . $queue;
        $options['exchange_type'] = 'direct';
        $options['exchange_routing_key'] = $queue;
        return parent::pushRaw($payload, $queue, $options);
    }
}
