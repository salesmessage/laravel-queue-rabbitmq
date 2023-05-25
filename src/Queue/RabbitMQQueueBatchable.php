<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue;

use PhpAmqpLib\Connection\AbstractConnection;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue as BaseRabbitMQQueue;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Channel\AMQPChannel;

class RabbitMQQueueBatchable extends BaseRabbitMQQueue
{
    protected function publishBasic($msg, $exchange = '', $destination = '', $mandatory = false, $immediate = false, $ticket = null): void
    {
        try {
            parent::publishBasic($msg, $exchange, $destination, $mandatory, $immediate, $ticket);
        } catch (AMQPConnectionClosedException|AMQPChannelClosedException) {
            $this->reconnect();
            parent::publishBasic($msg, $exchange, $destination, $mandatory, $immediate, $ticket);
        }
    }

    protected function publishBatch($jobs, $data = '', $queue = null): void
    {
        try {
            parent::publishBatch($jobs, $data, $queue);
        } catch (AMQPConnectionClosedException|AMQPChannelClosedException) {
            $this->reconnect();
            parent::publishBatch($jobs, $data, $queue);
        }
    }

    protected function createChannel(): AMQPChannel
    {
        try {
            return parent::createChannel();
        } catch (AMQPConnectionClosedException) {
            $this->reconnect();
            return parent::createChannel();
        }
    }

    public function push($job, $data = '', $queue = null)
    {
        $queue = $queue ?: $job->onQueue();
        return parent::push($job, $data, $queue);
    }


    public function pushRaw($payload, $queue = null, array $options = []): int|string|null
    {
        return parent::pushRaw($payload, $queue, $options);
    }
}
