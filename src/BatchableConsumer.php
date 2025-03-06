<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\RequestOptions;
use VladimirYuldashev\LaravelQueueRabbitMQ\Exceptions\MutexTimeout;
use VladimirYuldashev\LaravelQueueRabbitMQ\Interfaces\RabbitMQBatchable;
use GuzzleHttp\Client;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\WorkerOptions;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;

class BatchableConsumer extends Consumer
{
    /** @var array<string> $currentQueues runtime list of queues */
    protected array $currentQueues = [];
    /** @var int $currentQueue cursor for round-robin */
    protected int $currentQueue = 0;

    /** @var int $processedJob counter for batches */
    protected int $processedJob = 0;

    /** @var array $currentMessages current batch */
    protected array $currentMessages = [];

    /** @var string $mask mask of queues to listen */
    protected string $mask = '';

    /** @var bool $preCheck check queue before switch using internal rabbitmq API */
    protected bool $preCheck = true;

    /** @var bool $roundRobin switch between given queues by round-round */
    protected bool $roundRobin = true;

    /** @var int $processed counter processed jobs by current worker */
    private int $processed = 0;

    /** @var Queue */
    private Queue $queueConnection;

    /** @var string */
    private string $connectionName;
    /** @var string */
    private string $queue;
    /** @var WorkerOptions  */
    private WorkerOptions $options;

    /** @var int $currentPrefetch */
    private int $currentPrefetch = 0;

    /** @var int $currentConsumeInterval */
    private int $currentConsumeInterval = 0;

    /** @var bool $autoPrefetch */
    private bool $autoPrefetch = false;

    /** @var null|array $consumeIntervalMapping */
    private ?array $consumeIntervalMapping = null;

    /** @var AMQPMessage[] $pastQueuesMessages */
    private array $pastQueuesMessages = [];

    private ?int $workerExitCode = null;

    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected string $signature = 'rabbitmq:work';

    /**
     * The console command description.
     *
     * @var string
     */
    protected string $description = 'Consumer for rabbitmq queues using queue:work';

    /**
     * @param string $value
     */
    public function setMask(string $value): void
    {
        $this->mask = $value;
    }

    /**
     * @param bool $value
     */
    public function setPreCheck(bool $value): void
    {
        $this->preCheck = $value;
    }

    /**
     * @param bool $value
     */
    public function setRoundRobin(bool $value): void
    {
        $this->roundRobin = $value;
    }

    /**
     * @param bool $value
     */
    public function setAutoPrefetch(bool $value): void
    {
        $this->autoPrefetch = $value;
    }

    /**
     * @param array $value
     */
    public function setConsumeIntervalMapping(array $value): void
    {
        $this->consumeIntervalMapping = $value;
    }

    /**
     * @return string
     */
    public function getMask(): string
    {
        return $this->mask;
    }

    /**
     * Consumer logic
     *
     * @param string $connectionName
     * @param string $queue
     * @param WorkerOptions $options
     * @return int
     * @throws \Throwable
     */
    public function daemon($connectionName, $queue, WorkerOptions $options): int
    {
        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals();
        }

        $this->queue = $queue;
        $this->options = $options;
        $this->connectionName = $connectionName;

        $lastRestart = $this->getTimestampOfLastQueueRestart();

        [$startTime, $jobsProcessed] = [hrtime(true) / 1e9, 0];

        /** @var Queue $connection */
        $this->queueConnection = $this->manager->connection($this->connectionName);

        $heartbeatHandler = function () {
            $this->startHeartbeatCheck();
        };
        $mainHandler = function () use ($options, $queue, $lastRestart, $startTime, $jobsProcessed) {
            $this->start();
            while (true) {
                // Before reserving any jobs, we will make sure this queue is not paused and
                // if it is we will just pause this worker for a given amount of time and
                // make sure we do not need to kill this worker process off completely.
                if (!$this->daemonShouldRun($options, $this->connectionName, $queue)) {
                    $this->pauseWorker($options, $lastRestart);
                    continue;
                }

                // If the daemon should run (not in maintenance mode, etc.), then we can wait for a job.
                try {
                    if (!$this->channel?->getConnection()) {
                        logger()->info('RabbitMQConsumer.connection.broken.kill', [
                            'workerName' => $this->name,
                        ]);
                        $this->kill(self::EXIT_ERROR, $options);
                    }

                    $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
                    $this->channel->wait(null, false, $this->currentConsumeInterval);
                    $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);
                } catch (AMQPRuntimeException $exception) {
                    $this->exceptions->report($exception);

                    $this->kill(self::EXIT_ERROR, $options);
                } catch (AMQPTimeoutException) {
                    if ($this->currentPrefetch > 1) {
                        logger()->info('RabbitMQConsumer.prefetch.currentConsumeInterval.triggered', [
                            'consumeInterval' => $this->currentConsumeInterval,
                            'workerName' => $this->name,
                            'messagesReady' => count($this->currentMessages),
                            'currentPrefetchCount' => $this->currentPrefetch,
                        ]);
                        if ($this->roundRobin) {
                            $this->stopConsume();
                            $this->processBatch();
                            $this->switchToNextQueue();
                        } else {
                            $this->processBatch();
                        }
                    } else {
                        if ($this->roundRobin) {
                            logger()->info('RabbitMQConsumer.singlePrefetch.currentConsumeInterval.triggered', [
                                'workerName' => $this->name,
                                'messagesReady' => count($this->currentMessages),
                                'currentPrefetchCount' => $this->currentPrefetch,
                            ]);

                            $this->stopConsume();
                            $this->switchToNextQueue();
                        }
                    }
                } catch (\Exception | \Throwable $exception) {
                    $this->exceptions->report($exception);

                    $this->stopWorkerIfLostConnection($exception);
                }

                // Finally, we will check to see if we have exceeded our memory limits or if
                // the queue should restart based on other indications. If so, we'll stop
                // this worker and let whatever is "monitoring" it restart the process.
                $this->workerExitCode = $this->stopIfNecessary(
                    $options,
                    $lastRestart,
                    $startTime,
                    $jobsProcessed,
                    $this->currentJob
                );

                if (!is_null($this->workerExitCode)) {
                    return $this->stop($this->workerExitCode, $options);
                }
            }
        };

        $this->channel = $this->queueConnection->getChannel();

        if ($this->isAsyncMode()) {
            logger()->info('RabbitMQConsumer.AsyncMode.On');
            $resultStatus = 0;
            $coroutineContextHandler = function () use ($heartbeatHandler, $mainHandler, &$resultStatus) {
                logger()->info('RabbitMQConsumer.AsyncMode.Coroutines.Running');
                // we can't move it outside since Mutex should be created within coroutine context
                $this->connectionMutex = new Mutex(true);
                $heartbeatHandler();
                \go(function () use ($mainHandler, &$resultStatus) {
                    $resultStatus = $mainHandler();
                });
            };

            if (extension_loaded('swoole')) {
                logger()->info('RabbitMQConsumer.AsyncMode.Swoole');
                \Co\run($coroutineContextHandler);
            } elseif (extension_loaded('openswoole')) {
                logger()->info('RabbitMQConsumer.AsyncMode.OpenSwoole');
                \OpenSwoole\Runtime::enableCoroutine(true, \OpenSwoole\Runtime::HOOK_ALL);
                \co::run($coroutineContextHandler);
            } else {
                throw new \Exception('Async mode is not supported. Check if Swoole extension is installed');
            }
            return $resultStatus;
        } else {
            logger()->info('RabbitMQConsumer.AsyncMode.Off');
            $this->connectionMutex = new Mutex(false);
        }

        $heartbeatHandler();
        return $mainHandler();
    }

    /**
     * Start consuming the rabbitmq queues
     */
    private function start(): void
    {
        $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
        $this->channel->basic_qos(
            $this->prefetchSize,
            $this->prefetchCount,
            true
        );
        $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);
        $this->discoverQueues();
        if ($this->roundRobin) {
            $this->switchToNextQueue(true);
        } else {
            foreach ($this->currentQueues as $queue) {
                $this->startConsuming($queue);
            }
        }
    }

    /**
     * Switching to next queue(round-robin)
     *
     * @param bool $first
     */
    private function switchToNextQueue(bool $first = false)
    {
        logger()->info('RabbitMQConsumer.switchToNextQueue.before', [
            'workerName' => $this->name,
            'currentPrefetch' => $this->currentPrefetch,
            'currentMessagesCount' => count($this->currentMessages),
        ]);

        $nextQueue = $this->discoverNextQueue($first);
        $this->startConsuming($nextQueue);

        logger()->info('RabbitMQConsumer.switchToNextQueue.after', [
            'workerName' => $this->name,
            'currentPrefetch' => $this->currentPrefetch,
            'currentMessagesCount' => count($this->currentMessages),
        ]);
    }

    /**
     * Gets the next round-robin queue
     *
     * @param bool $first
     * @return string
     * @throws \Exception
     */
    private function nextQueueRoundRobin(bool $first = false): string
    {
        if (!$first) {
            $this->currentQueue++;
        } else {
            $this->currentQueue = 0;
        }
        $nextQueue = $this->currentQueues[$this->currentQueue] ?? null;
        if (!$nextQueue) {
            $this->discoverQueues();
            $this->currentQueue = 0;
            $nextQueue = $this->currentQueues[$this->currentQueue] ?? null;
            if (!$nextQueue) {
                throw new \Exception('Error next queue');
            }
        }

        return $nextQueue;
    }

    /**
     * Validates next queue if needed
     *
     * @param bool $first
     * @return string
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    private function discoverNextQueue(bool $first = false): string
    {
        do {
            $nextQueue = $this->nextQueueRoundRobin($first);
            $first = false;
            $queueIsNotReady = false;
            $this->currentPrefetch = $this->prefetchCount;
            $this->currentConsumeInterval = $this->consumeInterval;

            if ($this->preCheck || $this->autoPrefetch || $this->consumeIntervalMapping) {
                $client = $this->getHttpClient();

                $host = $this->config['hosts'][0]['host'];
                $port = $this->config['hosts'][0]['api_port'];
                $username = $this->config['hosts'][0]['user'];
                $password = $this->config['hosts'][0]['password'];

                $scheme = $this->config['secure'] ? 'https://' : 'http://';

                $url = $scheme . $host . ':' . $port;

                try {
                    $res = $client->get(
                        "$url/api/queues/%2F/$nextQueue", // %2F stands for /
                        [
                            'headers' => [
                                'Authorization' => 'Basic ' . base64_encode(
                                        $username . ':' . $password
                                    )
                            ]
                        ]
                    );
                } catch (RequestException $e) {
                    if ((int) $e->getCode() === 404) {
                        logger()->warning('RabbitMQConsumer.discoverNextQueue.queueNotFound', [
                            'queue' => $nextQueue,
                        ]);
                        $queueIsNotReady = true;
                        continue;
                    }
                    throw $e;
                }

                $queueData = json_decode($res->getBody());

                $messages = $queueData->messages_ready ?? 0;

                logger()->info('RabbitMQConsumer.queues.dataRetrieved', [
                    'queue' => $nextQueue,
                    'messagesReady' => $messages,
                    'totalMessages' => $queueData->messages ?? 0,
                ]);

                if ($this->preCheck) {
                    if ($messages === 0 || ($queueData->consumers ?? 0) >= 2) {
                        $queueIsNotReady = true;
                        logger()->info('RabbitMQConsumer.queues.precheck.failed', [
                            'queue' => $nextQueue,
                            'workerName' => $this->name,
                            'messagesReady' => $messages,
                            'active-consumers' => $queueData->consumers ?? 0,
                        ]);
                    }
                }

                if ($this->autoPrefetch && $messages > 0) {
                    $this->currentPrefetch = $messages >= $this->prefetchCount ? $this->prefetchCount : $messages;
                    logger()->info('RabbitMQConsumer.queues.currentPrefetch.set', [
                        'queue' => $nextQueue,
                        'workerName' => $this->name,
                        'prefetchCount' => $this->prefetchCount,
                        'currentPrefetch' => $this->currentPrefetch,
                        'messagesReady' => $messages
                    ]);
                }

                if ($this->consumeIntervalMapping) {
                    foreach ($this->consumeIntervalMapping as $mapping) {
                        if ($mapping['range'] >= $messages) {
                            $this->currentConsumeInterval = (int) $mapping['interval'];
                            logger()->info('RabbitMQConsumer.queues.currentConsumeInterval.set', [
                                'queue' => $nextQueue,
                                'workerName' => $this->name,
                                'currentConsumeInterval' => $this->currentConsumeInterval,
                                'messagesReady' => $queueData->messages_ready
                            ]);
                            break;
                        }
                    }
                }
            }
        } while ($queueIsNotReady);

        return $nextQueue;
    }

    /**
     * Batch handler
     *
     * @param AMQPMessage $message
     */
    private function batchHandler(AMQPMessage $message)
    {
        logger()->info('RabbitMQConsumer.batchHandler.addingCurrentMessage', [
            'workerName' => $this->name,
            'existingMessagesCount' => count($this->currentMessages),
            'routingKey' => $message->getRoutingKey(),
            'currentPrefetch' => $this->currentPrefetch,
            'processedJob' => $this->processedJob,
        ]);

        $this->currentMessages[] = $message;
        $this->processedJob++;
        if ($this->processedJob >= $this->currentPrefetch) {
            if ($this->roundRobin) {
                $this->stopConsume();
                $this->processBatch();
                $this->switchToNextQueue();
            } else {
                $this->processBatch();
            }
        }
    }

    /**
     * Single message handler
     *
     * @param AMQPMessage $message
     */
    private function singleHandler(AMQPMessage $message)
    {
        if ($this->roundRobin) {
            $this->stopConsume();
            $this->processMessage($message);
            $this->switchToNextQueue();
        } else {
            $this->processMessage($message);
        }
    }

    /**
     * AMQP consuming logic
     *
     * @param string $queue
     */
    private function startConsuming(string $queue)
    {
        $callback = function (AMQPMessage $message) use ($queue, &$callback): void {
            if (!$this->isValidMessage($message, $queue)) {
                return;
            }

            if ($this->currentPrefetch > 1) {
                $this->batchHandler($message);
            } else {
                $this->singleHandler($message);
            }
        };

        logger()->info('RabbitMQConsumer.queues.startConsuming', [
            'workerName' => $this->name,
            'newQueue' => $queue
        ]);

        $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
        $this->channel->basic_consume(
            $queue,
            $this->consumerTag,
            false,
            false,
            false,
            false,
            $callback
        );
        $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);
    }

    /**
     * StopConsume command (switch queue/exit)
     */
    private function stopConsume()
    {
        logger()->info('RabbitMQConsumer.StopConsume', [
            'workerName' => $this->name,
            'currentMessagesCount' => count($this->currentMessages),
            'currentPrefetch' => $this->currentPrefetch,
        ]);

        $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
        $this->channel->basic_cancel($this->consumerTag, true);
        $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);
    }

    /**
     * Discovers existing queues via API
     *
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    private function discoverQueues()
    {
        do {
            $host = $this->config['hosts'][0]['host'];
            $port = $this->config['hosts'][0]['api_port'];
            $username = $this->config['hosts'][0]['user'];
            $password = $this->config['hosts'][0]['password'];
            $client = $this->getHttpClient();
            $scheme = $this->config['secure'] ? 'https://' : 'http://';
            $url = $scheme . $host . ':' . $port;
            $res = $client->get(
                "$url/api/queues?columns=messages_unacknowledged,messages_ready,message_bytes_unacknowledged,message_bytes_ready,name,policy,idle_since,consumers,state,consumer_details.consumer_tag,consumer_details.channel_details.connection_name,consumer_details.channel_details.name,consumers",
                [
                    'headers' => [
                        'Authorization' => 'Basic ' . base64_encode(
                                $username . ':' . $password
                            )
                    ]
                ]
            );
            $queues = json_decode($res->getBody());
            $queues = collect($queues)
                ->filter(function ($queue) {
                    $readyMessages = $queue->messages_ready ?? 0;
                    return str_starts_with(
                            $queue->name,
                            $this->mask
                        ) && (($this->roundRobin && $readyMessages > 0) || !$this->roundRobin);
                })
                ->pluck('name')
                ->values();

            // Shuffle $queues collection randomly
            if ($this->roundRobin) {
                $queues = $queues->shuffle();
            }

            $this->currentQueues = $queues->toArray();

            if (count($this->currentQueues) === 0) {
                $this->sleep($this->options->sleep);
            } else {
                logger()->info('RabbitMQConsumer.queues.discovered', [
                    'workerName' => $this->name,
                    'queues' => collect($queues)->implode(', ')
                ]);
            }
        } while (count($this->currentQueues) === 0);
    }

    /**
     * Process batch messages
     */
    private function processBatch()
    {
        $this->processJobs();
        $this->processedJob = 0;
        $this->currentMessages = [];

        $this->requeuePastQueuesMessages();

        logger()->info('RabbitMQConsumer.BatchProcessed.ClearData', [
            'workerName' => $this->name,
        ]);
    }

    /**
     * Gets job class by AMQPMessage
     *
     * @param AMQPMessage $message
     * @return mixed
     */
    private function getJobByMessage(AMQPMessage $message): mixed
    {
        $jobClass = $this->queueConnection->getJobClass();
        /** @var RabbitMQJob $job */
        return new $jobClass(
            $this->container,
            $this->queueConnection,
            $message,
            $this->connectionName,
            $this->queue
        );
    }

    /**
     * Single message process logic
     *
     * @param AMQPMessage $message
     */
    private function processMessage(AMQPMessage $message)
    {
        $job = $this->getJobByMessage($message);

        if ($this->supportsAsyncSignals()) {
            $this->registerTimeoutHandler($job, $this->options);
        }

        logger()->info('RabbitMQConsumer.processMessage.before', [
            'workerName' => $this->name,
            'jobsClass' => $job->getPayloadClass(),
            'routingKey' => $message->getRoutingKey(),
            'currentMessagesCount' => count($this->currentMessages),
            'prefetchCount' => $this->currentPrefetch,
        ]);

        $this->runJob($job, $this->connectionName, $this->options);

        if ($this->supportsAsyncSignals()) {
            $this->resetTimeoutHandler();
        }

        $this->requeuePastQueuesMessages();
    }

    private function isValidMessage(AMQPMessage $message, string $currentQueue): bool
    {
        // case when we receive messages from previously consumed queue
        // it might be possible since for cancelling consuming we don't wait for response
        if ($message->getRoutingKey() !== $currentQueue) {
            logger()->warning('RabbitMQConsumer.messageFromPastQueue', [
                'workerName' => $this->name,
                'currentQueue' => $currentQueue,
                'receivedQueue' => $message->getRoutingKey(),
            ]);
            // we don't send message back immediately to not process it again (in the theory)
            // instead we collect it and send when we finished messages processing
            $this->pastQueuesMessages[] = $message;
            // if for some reason we collect a lot of messages from the different queues
            if (count($this->pastQueuesMessages) >= $this->prefetchCount) {
                logger()->warning('RabbitMQConsumer.messageFromPastQueue.moreThanPrefetchCount', [
                    'workerName' => $this->name,
                    'currentQueue' => $currentQueue,
                    'prefetchCount' => $this->prefetchCount,
                    'pastQueuesMessagesCount' => count($this->pastQueuesMessages),
                ]);
            }
            return false;
        }

        return true;
    }

    private function requeuePastQueuesMessages(): void
    {
        if (!$this->pastQueuesMessages) {
            return;
        }

        logger()->warning('RabbitMQConsumer.requeuePastQueuesMessages', [
            'workerName' => $this->name,
            'pastQueuesMessagesCount' => count($this->pastQueuesMessages),
            'pastQueues' => array_unique(
                array_map(fn (AMQPMessage $pastQueueMessage) => $pastQueueMessage->getRoutingKey(), $this->pastQueuesMessages)
            ),
        ]);

        $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
        foreach ($this->pastQueuesMessages as $message) {
            try {
                $message->nack(true);
            } catch (\Throwable $e) {
                logger()->error('RabbitMQConsumer.requeuePastQueuesMessages.failed', [
                    'workerName' => $this->name,
                    'messageQueue' => $message->getRoutingKey(),
                    'errorMessage' => $e->getMessage(),
                    'errorTrace' => $e->getTraceAsString(),
                ]);
            }
        }
        $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);
        $this->pastQueuesMessages = [];
    }

    /**
     * Process current batch
     */
    private function processJobs()
    {
        if (count($this->currentMessages) === 0) {
            logger()->warning('RabbitMQConsumer.jobs.empty', [
                'workerName' => $this->name,
            ]);
            return;
        }

        $class = $this->getJobByMessage($this->currentMessages[0])->getPayloadClass();

        $sameClass = collect($this->currentMessages)->every(function (AMQPMessage $message) use ($class) {
            return $this->getJobByMessage($message)->getPayloadClass() === $class;
        });

        $reflection = new \ReflectionClass($class);

        if (count($this->currentMessages) > 1 && $sameClass && $reflection->implementsInterface(
                RabbitMQBatchable::class
            )) {
            $batchData = [];
            foreach ($this->currentMessages as $message) {
                $job = $this->getJobByMessage($message);
                $batchData[] = $job->getPayloadData();
            }

            $routingKeys = array_map(fn ($currentMessage) => $currentMessage->getRoutingKey(), $this->currentMessages);
            $routingKeys = array_unique($routingKeys);
            if (count($routingKeys) > 1) {
                logger()->warning('RabbitMQConsumer.IncorrectGroupedRoutingKeys', [
                    'workerName' => $this->name,
                    'routingKeys' => $routingKeys,
                ]);
            }

            $failed = false;
            try {
                $class::collection($batchData);
                logger()->info('RabbitMQConsumer.jobs.process.done', [
                    'workerName' => $this->name,
                    'jobsCount' => count($this->currentMessages),
                    'jobsClass' => $class,
                    'routingKey' => $this->currentMessages[0]->getRoutingKey(),
                    'currentPrefetchCount' => $this->currentPrefetch,
                    'currentQueue' => $this->currentQueues[$this->currentQueue] ?? null,
                ]);
            } catch (\Throwable $exception) {
                $failed = true;
                logger()->error('RabbitMQConsumer.batch.process.failed', [
                    'workerName' => $this->name,
                    'message' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString(),
                    'jobsClass' => $class,
                    'routingKey' => $this->currentMessages[0]->getRoutingKey(),
                    'currentPrefetchCount' => $this->currentPrefetch,
                    'currentQueue' => $this->currentQueues[$this->currentQueue] ?? null,
                ]);
            }
            $this->connectionMutex->lock(static::MAIN_HANDLER_LOCK);
            /** @var AMQPMessage $message */
            foreach ($this->currentMessages as $message) {
                if ($failed) {
                    $this->processMessage($message);
                } else {
                    try {
                        $message->ack(); // TODO group ack
                    } catch (\Throwable $exception) {
                        logger()->error('RabbitMQConsumer.ack.failed', [
                            'workerName' => $this->name,
                            'message' => $exception->getMessage(),
                            'trace' => $exception->getTraceAsString(),
                            'jobsClass' => $class,
                            'routingKey' => $message->getRoutingKey()
                        ]);
                    }
                }
            }
            $this->connectionMutex->unlock(static::MAIN_HANDLER_LOCK);
            $this->processed += count($this->currentMessages);
            return;
        }

        /** @var AMQPMessage $message */
        foreach ($this->currentMessages as $message) {
            $this->processMessage($message);
        }
        $this->processed += count($this->currentMessages);
    }

    private function startHeartbeatCheck(): void
    {
        if (!$this->isAsyncMode()) {
            return;
        }
        $heartbeatInterval = (int) ($this->config['options']['heartbeat'] ?? 0);
        if (!$heartbeatInterval) {
            return;
        }

        $heartbeatHandler = function () {
            if ($this->shouldQuit || !is_null($this->workerExitCode)) {
                return;
            }

            try {
                $connection = $this->channel->getConnection();
                if (!$connection?->isConnected()
                    || $connection->isWriting()
                    || $connection->isBlocked()
                ) {
                    return;
                }

                $this->connectionMutex->lock(static::HEALTHCHECK_HANDLER_LOCK, 3);
                $connection->checkHeartBeat();
            } catch (MutexTimeout) {
            } catch (\Throwable $e) {
                logger()->error('RabbitMQConsumer.heartbeatCheck.error', [
                    'message' => $e->getMessage(),
                    'trace' => $e->getTraceAsString(),
                    'workerName' => $this->name,
                ]);
                $this->shouldQuit = true;
            } finally {
                $this->connectionMutex->unlock(static::HEALTHCHECK_HANDLER_LOCK);
            }
        };

        \go(function () use ($heartbeatHandler, $heartbeatInterval) {
            logger()->info('RabbitMQConsumer.heartbeatCheck.started');
            while (true) {
                sleep($heartbeatInterval);
                $heartbeatHandler();
                if ($this->shouldQuit || !is_null($this->workerExitCode)) {
                    return;
                }
            }
        });
    }

    /**
     * @return Client
     */
    private function getHttpClient(): Client
    {
        return new Client([
            RequestOptions::TIMEOUT => 30,
            RequestOptions::CONNECT_TIMEOUT => 30,
        ]);
    }
}
