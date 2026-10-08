<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Unit\Queue;

use PHPUnit\Framework\TestCase;
use ReflectionClass;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;

class DelayQueueArgumentsTest extends TestCase
{
    public function test_delay_queue_arguments_have_no_x_expires(): void
    {
        $queue = (new ReflectionClass(DelayArgumentsQueueStub::class))->newInstanceWithoutConstructor();

        $arguments = $queue->delayArguments('some-queue', 60000);

        $this->assertSame([
            'x-dead-letter-exchange' => 'some-exchange',
            'x-dead-letter-routing-key' => 'some-queue',
            'x-message-ttl' => 60000,
        ], $arguments);
        $this->assertArrayNotHasKey('x-expires', $arguments);
    }
}

class DelayArgumentsQueueStub extends RabbitMQQueue
{
    protected function getExchange(?string $exchange = null): string
    {
        return 'some-exchange';
    }

    protected function getRoutingKey(string $destination): string
    {
        return $destination;
    }

    public function delayArguments(string $destination, int $ttl): array
    {
        return $this->getDelayQueueArguments($destination, $ttl);
    }
}
