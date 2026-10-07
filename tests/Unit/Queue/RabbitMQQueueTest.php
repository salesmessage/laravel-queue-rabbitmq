<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Unit\Queue;

use Illuminate\Support\Facades\Log;
use Mockery;
use Mockery\MockInterface;
use Orchestra\Testbench\TestCase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Wire\AMQPTable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Exceptions\DelayTooLongException;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\QueueConfig;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\QueueConfigFactory;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;

class RabbitMQQueueTest extends TestCase
{
    private const QUEUE = 'orders';

    private const JOB = 'App\\Jobs\\SendReport';

    private MockInterface $channel;

    /** @var list<string> */
    private array $declared = [];

    /** @var array<string, array> */
    private array $declaredArguments = [];

    protected function setUp(): void
    {
        parent::setUp();

        $this->channel = Mockery::spy(AMQPChannel::class);
        $this->channel->shouldReceive('queue_declare')->andReturnUsing(function (string $name, ...$rest) {
            $this->declared[] = $name;
            $arguments = $rest[5] ?? [];
            $this->declaredArguments[$name] = ($arguments instanceof AMQPTable) ? $arguments->getNativeData() : $arguments;

            return null;
        });
    }

    public function test_push_raw_declares_the_queue_on_every_publish(): void
    {
        $queue = $this->queue();

        $queue->pushRaw($this->payload(), self::QUEUE);
        $queue->pushRaw($this->payload(), self::QUEUE);

        $this->assertSame([self::QUEUE, self::QUEUE], $this->declared);
        $this->channel->shouldHaveReceived('basic_publish')->twice();
    }

    public function test_later_raw_declares_the_target_and_the_delay_queue_on_every_publish(): void
    {
        $queue = $this->queue();

        $queue->laterRaw(60, $this->payload(), self::QUEUE);
        $queue->laterRaw(60, $this->payload(), self::QUEUE);

        $delayQueue = 'delay.60000.'.self::QUEUE;
        $this->assertSame([self::QUEUE, $delayQueue, self::QUEUE, $delayQueue], $this->declared);
        $this->channel->shouldHaveReceived('basic_publish')
            ->with(Mockery::any(), null, $delayQueue, true, false, null)
            ->twice();
    }

    public function test_bulk_raw_declares_the_queue_on_every_publish(): void
    {
        $queue = $this->queue();

        $queue->bulkRaw($this->payload(), self::QUEUE);
        $queue->bulkRaw($this->payload(), self::QUEUE);

        $this->assertSame([self::QUEUE, self::QUEUE], $this->declared);
        $this->channel->shouldHaveReceived('batch_basic_publish')->twice();
    }

    public function test_delay_above_the_cap_is_logged_and_still_published_in_log_mode(): void
    {
        Log::shouldReceive('error')->once()->with('RabbitMQQueue.laterRaw.delayTooLong', [
            'queue' => self::QUEUE,
            'job' => self::JOB,
            'delay_seconds' => 7200,
            'max_delay_seconds' => 3600,
        ]);

        $this->queue(['max_delay_seconds' => 3600])->laterRaw(7200, $this->payload(), self::QUEUE);

        $this->channel->shouldHaveReceived('basic_publish')->once();
    }

    public function test_delay_above_the_cap_throws_and_publishes_nothing_in_throw_mode(): void
    {
        $queue = $this->queue([
            'max_delay_seconds' => 3600,
            'max_delay_mode' => QueueConfig::MAX_DELAY_MODE_THROW,
        ]);

        try {
            $queue->laterRaw(7200, $this->payload(), self::QUEUE);
            $this->fail('DelayTooLongException was not thrown.');
        } catch (DelayTooLongException $exception) {
            $this->assertSame(
                'Delay of 7200 seconds for queue "orders" exceeds the maximum of 3600 seconds.',
                $exception->getMessage()
            );
        }

        $this->assertSame([], $this->declared);
        $this->channel->shouldNotHaveReceived('basic_publish');
    }

    public function test_delay_above_the_cap_is_clamped_to_the_cap_and_logged_in_clamp_mode(): void
    {
        Log::shouldReceive('warning')->once()->with('RabbitMQQueue.laterRaw.delayClamped', [
            'queue' => self::QUEUE,
            'job' => self::JOB,
            'delay_seconds' => 7200,
            'max_delay_seconds' => 3600,
        ]);
        Log::shouldReceive('error')->never();

        $this->queue(['max_delay_seconds' => 3600, 'max_delay_mode' => QueueConfig::MAX_DELAY_MODE_CLAMP])
            ->laterRaw(7200, $this->payload(), self::QUEUE);

        $delayQueue = 'delay.3600000.'.self::QUEUE;
        $this->assertSame([self::QUEUE, $delayQueue], $this->declared);
        $this->assertSame(3600000, $this->declaredArguments[$delayQueue]['x-message-ttl']);
        $this->channel->shouldHaveReceived('basic_publish')
            ->with(Mockery::any(), null, $delayQueue, true, false, null)
            ->once();
    }

    public function test_delay_equal_to_the_cap_is_allowed(): void
    {
        Log::shouldReceive('error')->never();

        $this->queue(['max_delay_seconds' => 3600, 'max_delay_mode' => QueueConfig::MAX_DELAY_MODE_THROW])
            ->laterRaw(3600, $this->payload(), self::QUEUE);

        $this->channel->shouldHaveReceived('basic_publish')->once();
    }

    public function test_zero_max_delay_disables_the_cap(): void
    {
        Log::shouldReceive('error')->never();

        $this->queue(['max_delay_seconds' => 0, 'max_delay_mode' => QueueConfig::MAX_DELAY_MODE_THROW])
            ->laterRaw(7 * 86400, $this->payload(), self::QUEUE);

        $this->channel->shouldHaveReceived('basic_publish')->once();
    }

    public function test_immediate_later_raw_is_not_checked_against_the_cap(): void
    {
        Log::shouldReceive('error')->never();

        $this->queue(['max_delay_seconds' => 1, 'max_delay_mode' => QueueConfig::MAX_DELAY_MODE_THROW])
            ->laterRaw(0, $this->payload(), self::QUEUE);

        $this->assertSame([self::QUEUE], $this->declared);
    }

    private function queue(array $queueOptions = []): RabbitMQQueue
    {
        $connection = Mockery::mock(AbstractConnection::class);
        $connection->shouldReceive('channel')->andReturn($this->channel);

        return (new RabbitMQQueue(QueueConfigFactory::make([
            'queue' => 'default',
            'options' => ['queue' => $queueOptions],
        ])))->setConnection($connection);
    }

    private function payload(): string
    {
        return json_encode([
            'id' => 'job-id',
            'displayName' => self::JOB,
            'job' => 'Illuminate\\Queue\\CallQueuedHandler@call',
            'data' => [],
        ]);
    }
}
