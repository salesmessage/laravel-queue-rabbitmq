<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ;

use Exception;
use Illuminate\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Factory as QueueManager;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use PhpAmqpLib\Channel\AbstractChannel;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Message\AMQPMessage;
use Throwable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Contracts\ChannelPool;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Connection\ChannelPoolFactory;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;

class Consumer extends Worker
{
    /** @var Container */
    protected $container;

    /** @var string */
    protected $consumerTag;

    /** @var int */
    protected $prefetchSize;

    /** @var int */
    protected $maxPriority;

    /** @var int */
    protected $prefetchCount;

    /** @var object|null */
    protected $currentJob;

    /** @var array  */
    protected array $config;

    protected bool $asyncMode = false;

    protected int $consumeInterval = 60;

    /**
     * Do not access this property directly. Use @see self::popChannel() to get access
     * and to return channel again to pool use @see self::pushChannel($data)
     * @var AMQPChannel
     */
    private $channel;

    /**
     * Stack with 1 variable (channel). Provides access to the "channel" only from the 1 code place at the same time.
     * Used for async handlers (coroutines), when coroutines need to use "channel" they have to wait for while another coroutine has been finished.
     * @var ChannelPool|null
     */
    private ChannelPool|null $channelPool = null;

    /**
     * Indicates if channel has been taken from the pool to work in the main handler (process)
     * Used as indicator if we need a fallback channel (channel from class property)
     * @var bool
     */
    private bool $isMainHandlerUseChannelNow = false;

    /**
     * Create a new queue worker.
     *
     * @param  \Illuminate\Contracts\Queue\Factory  $manager
     * @param  \Illuminate\Contracts\Events\Dispatcher  $events
     * @param  \Illuminate\Contracts\Debug\ExceptionHandler  $exceptions
     * @param  callable  $isDownForMaintenance
     * @param  callable|null  $resetScope
     * @return void
     */
    public function __construct(QueueManager $manager,
        Dispatcher $events,
        ExceptionHandler $exceptions,
        callable $isDownForMaintenance,
        callable $resetScope = null,
        array $config
    )
    {
        $this->events = $events;
        $this->manager = $manager;
        $this->exceptions = $exceptions;
        $this->isDownForMaintenance = $isDownForMaintenance;
        $this->resetScope = $resetScope;
        $this->config = $config;
    }

    public function setContainer(Container $value): void
    {
        $this->container = $value;
    }

    public function setConsumerTag(string $value): void
    {
        $this->consumerTag = $value;
    }

    public function setMaxPriority(int $value): void
    {
        $this->maxPriority = $value;
    }

    public function setPrefetchSize(int $value): void
    {
        $this->prefetchSize = $value;
    }

    public function setPrefetchCount(int $value): void
    {
        $this->prefetchCount = $value;
    }

    /**
     * @return bool
     */
    public function isAsyncMode(): bool
    {
        return $this->asyncMode;
    }

    /**
     * @param bool $asyncMode
     */
    public function setAsyncMode(bool $asyncMode): void
    {
        $this->asyncMode = $asyncMode;
    }

    /**
     * @param int $consumeInterval
     */
    public function setConsumeInterval(int $consumeInterval): void
    {
        $this->consumeInterval = $consumeInterval;
    }

    /**
     * Listen to the given queue in a loop.
     *
     * @param  string  $connectionName
     * @param  string  $queue
     * @return int
     *
     * @throws Throwable
     */
    public function daemon($connectionName, $queue, WorkerOptions $options)
    {
        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals();
        }

        $lastRestart = $this->getTimestampOfLastQueueRestart();

        [$startTime, $jobsProcessed] = [hrtime(true) / 1e9, 0];

        /** @var RabbitMQQueue $connection */
        $connection = $this->manager->connection($connectionName);

        $this->initializeChannelPool($connection->getChannel());
        $channel = $this->popChannel();

        $channel->basic_qos(
            $this->prefetchSize,
            $this->prefetchCount,
            false
        );

        $jobClass = $connection->getJobClass();
        $arguments = [];
        if ($this->maxPriority) {
            $arguments['priority'] = ['I', $this->maxPriority];
        }

        $channel->basic_consume(
            $queue,
            $this->consumerTag,
            false,
            false,
            false,
            false,
            function (AMQPMessage $message) use ($connection, $options, $connectionName, $queue, $jobClass, &$jobsProcessed): void {
                $job = new $jobClass(
                    $this->container,
                    $connection,
                    $message,
                    $connectionName,
                    $queue
                );

                $this->currentJob = $job;

                if ($this->supportsAsyncSignals()) {
                    $this->registerTimeoutHandler($job, $options);
                }

                $jobsProcessed++;

                $this->runJob($job, $connectionName, $options);

                if ($this->supportsAsyncSignals()) {
                    $this->resetTimeoutHandler();
                }

                if ($options->rest > 0) {
                    $this->sleep($options->rest);
                }
            },
            null,
            $arguments
        );

        while ($channel->is_consuming()) {
            // Before reserving any jobs, we will make sure this queue is not paused and
            // if it is we will just pause this worker for a given amount of time and
            // make sure we do not need to kill this worker process off completely.
            if (! $this->daemonShouldRun($options, $connectionName, $queue)) {
                $this->pauseWorker($options, $lastRestart);

                continue;
            }

            // If the daemon should run (not in maintenance mode, etc.), then we can wait for a job.
            try {
                $channel->wait(null, true, (int) $options->timeout);
            } catch (AMQPRuntimeException $exception) {
                $this->exceptions->report($exception);

                $this->kill(self::EXIT_ERROR, $options);
            } catch (Exception|Throwable $exception) {
                $this->exceptions->report($exception);

                $this->stopWorkerIfLostConnection($exception);
            }

            // If no job is got off the queue, we will need to sleep the worker.
            if ($this->currentJob === null) {
                $this->sleep($options->sleep);
            }

            // Finally, we will check to see if we have exceeded our memory limits or if
            // the queue should restart based on other indications. If so, we'll stop
            // this worker and let whatever is "monitoring" it restart the process.
            $status = $this->stopIfNecessary(
                $options,
                $lastRestart,
                $startTime,
                $jobsProcessed,
                $this->currentJob
            );

            if (! is_null($status)) {
                $this->pushChannel($channel);
                return $this->stop($status, $options);
            }

            $this->currentJob = null;
        }
    }

    /**
     * Determine if the daemon should process on this iteration.
     *
     * @param  string  $connectionName
     * @param  string  $queue
     */
    protected function daemonShouldRun(WorkerOptions $options, $connectionName, $queue): bool
    {
        return ! ((($this->isDownForMaintenance)() && ! $options->force) || $this->paused);
    }

    /**
     * Stop listening and bail out of the script.
     *
     * @param  int  $status
     * @param  WorkerOptions|null  $options
     * @return int
     */
    public function stop($status = 0, $options = null)
    {
        $channel = $this->popChannel();
        // Tell the server you are going to stop consuming.
        // It will finish up the last message and not send you any more.
        $channel->basic_cancel($this->consumerTag, false, true);
        $this->pushChannel($channel);

        return parent::stop($status, $options);
    }

    protected function popChannel(bool $isMainHandlerNeedIt = true, float $timeout = null): ?AbstractChannel
    {
        /**
         * Explanation of combination "isEmpty" + "isMainHandlerUseChannelNow":
         * if pool is empty and channel is used in the main process and other part of code needs channel
         * for communication, we just use it from class "property".
         * this case is possible when in "while" loop we "pop" channel from the pool. pool is empty,
         * but in the "wait" (channel's method) here is callback that needs to "pop" channel again.
         * to not stuck (waiting for channel from the pool) we just provide channel from class property,
         * since implementation with channels pool has effect only for different coroutines to make 1 coroutine
         * waiting for another coroutine has been finished (finished using of channel)
         */
        $channel = $isMainHandlerNeedIt && $this->channelPool->isEmpty() && $this->isMainHandlerUseChannelNow
            ? $this->channel
            : $this->channelPool->pop($timeout);
        $this->isMainHandlerUseChannelNow = $isMainHandlerNeedIt;
        return $channel;
    }

    protected function pushChannel($data): void
    {
        if ($this->channelPool->isEmpty()) {
            $this->channelPool->push($data);
        }
        $this->isMainHandlerUseChannelNow = false;
    }

    protected function initializeChannelPool(AbstractChannel $channel): void
    {
        if (!$this->channelPool) {
            $this->channelPool = ChannelPoolFactory::make($this->isAsyncMode());
            $this->channelPool->push($channel);
            $this->channel = $channel;
        }
    }
}
