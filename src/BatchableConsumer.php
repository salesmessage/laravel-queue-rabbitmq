<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ;

use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Factory as QueueManager;
use VladimirYuldashev\LaravelQueueRabbitMQ\Interfaces\RabbitMQBatchable;
use GuzzleHttp\Client;
use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\WorkerOptions;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use VladimirYuldashev\LaravelQueueRabbitMQ\Consumer;
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
    private Queue $connection;

    /** @var string */
    private string $connectionName;
    /** @var string */
    private string $queue;
    /** @var WorkerOptions  */
    private WorkerOptions $options;

    /** @var int $currentPrefetch */
    private int $currentPrefetch = 0;

    /** @var int $currentTimeout */
    private int $currentTimeout = 0;

    /** @var bool $autoPrefetch */
    private bool $autoPrefetch = false;

    /** @var null|array $timeoutsMapping */
    private ?array $timeoutsMapping = null;


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
    public function setTimeoutsMapping(array $value): void
    {
        $this->timeoutsMapping = $value;
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
        $this->connection = $this->manager->connection($this->connectionName);

        $this->channel = $this->connection->getChannel();

        $arguments = [];
        if ($this->maxPriority) {
            $arguments['priority'] = ['I', $this->maxPriority];
        }

        $this->start();
        $this->startHearbeatCheck();

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
                if (!$this->channel->getConnection()) {
                    logger()->info('RabbitMQConsumer.connection.broken.kill', [
                        'workerName' => $this->name,
                    ]);
                    $this->kill(self::EXIT_ERROR, $options);
                    continue;
                }

                $this->channel->wait(null, false, $this->currentTimeout);
            } catch (AMQPRuntimeException $exception) {
                $this->exceptions->report($exception);

                $this->kill(self::EXIT_ERROR, $options);
            } catch (AMQPTimeoutException) {
                if ($this->currentPrefetch > 1) {
                    logger()->info('RabbitMQConsumer.prefetch.timeout', [
                        'timeout' => $this->currentTimeout,
                        'workerName' => $this->name,
                        'messagesReady' => count($this->currentMessages)
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
            $status = $this->stopIfNecessary(
                $options,
                $lastRestart,
                $startTime,
                $jobsProcessed,
                $this->currentJob
            );

            if (!is_null($status)) {
                return $this->stop($status, $options);
            }
        }

        $this->channel->close();
        $this->connection->close();
    }

    /**
     * Start consuming the rabbitmq queues
     */
    private function start(): void
    {
        $this->channel->basic_qos(
            $this->prefetchSize,
            $this->prefetchCount,
            true
        );
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
        $nextQueue = $this->discoverNextQueue($first);
        $this->startConsuming($nextQueue);
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
            $queueIsNotReady = false;
            $this->currentPrefetch = $this->prefetchCount;
            $this->currentTimeout = (int) $this->options->timeout;

            if ($this->preCheck || $this->autoPrefetch || $this->timeoutsMapping) {
                $client = new Client();

                $host = $this->config['hosts'][0]['host'];
                $port = $this->config['hosts'][0]['api_port'];
                $username = $this->config['hosts'][0]['user'];
                $password = $this->config['hosts'][0]['password'];

                $scheme = $this->config['secure'] ? 'https://' : 'http://';

                $url = $scheme . $host . ':' . $port;
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
                $queueData = json_decode($res->getBody());

                $messages = $queueData->messages_ready ?? 0;

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

                if ($this->autoPrefetch) {
                    $this->currentPrefetch = ($queueData->messages_ready ?? $this->prefetchCount) >= $this->prefetchCount ? $this->prefetchCount : $queueData->messages_ready;
                    logger()->info('RabbitMQConsumer.queues.currentPrefetch.set', [
                        'queue' => $nextQueue,
                        'workerName' => $this->name,
                        'currentPrefetch' => $this->currentPrefetch,
                        'messagesReady' => $messages
                    ]);
                }

                if ($this->timeoutsMapping) {
                    foreach ($this->timeoutsMapping as $mapping) {
                        if ($mapping['range'] >= $messages) {
                            $this->currentTimeout = $mapping['timeout'];
                            logger()->info('RabbitMQConsumer.queues.currentTimeout.set', [
                                'queue' => $nextQueue,
                                'workerName' => $this->name,
                                'currentTimeout' => $this->currentTimeout,
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
            if ($this->currentPrefetch > 1) {
                $this->batchHandler($message);
            } else {
                $this->singleHandler($message);
            }
        };

        $this->channel->basic_consume(
            $queue,
            $this->consumerTag,
            false,
            false,
            false,
            false,
            $callback
        );
    }

    /**
     * StopConsume command (switch queue/exit)
     */
    private function stopConsume()
    {
        $this->channel->basic_cancel($this->consumerTag, true);
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
            $client = new Client();
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
    }

    /**
     * Gets job class by AMQPMessage
     *
     * @param AMQPMessage $message
     * @return mixed
     */
    private function getJobByMessage(AMQPMessage $message): mixed
    {
        $jobClass = $this->connection->getJobClass();
        /** @var RabbitMQJob $job */
        return new $jobClass(
            $this->container,
            $this->connection,
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
            'routingKey' => $message->getRoutingKey()
        ]);

        $this->runJob($job, $this->connectionName, $this->options);

        if ($this->supportsAsyncSignals()) {
            $this->resetTimeoutHandler();
        }
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

            $failed = false;
            try {
                $class::collection($batchData);
                logger()->info('RabbitMQConsumer.jobs.process.done', [
                    'workerName' => $this->name,
                    'jobsCount' => count($this->currentMessages),
                    'jobsClass' => $class,
                    'routingKey' => $this->currentMessages[0]->getRoutingKey()
                ]);
            } catch (\Throwable $exception) {
                $failed = true;
                logger()->warning('RabbitMQConsumer.batch.process.failed', [
                    'workerName' => $this->name,
                    'message' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString(),
                    'jobsClass' => $class,
                    'routingKey' => $this->currentMessages[0]->getRoutingKey()
                ]);
            }
            foreach ($this->currentMessages as $message) {
                if ($failed) {
                    $this->processMessage($message);
                } else {
                    $this->connection->getChannel()->basic_ack($message->getDeliveryTag()); // TODO grouped ack test
                }
            }
            $this->processed += count($this->currentMessages);
            return;
        }

        /** @var AMQPMessage $message */
        foreach ($this->currentMessages as $message) {
            $this->processMessage($message);
        }
        $this->processed += count($this->currentMessages);
    }

    private function startHearbeatCheck()
    {
        $hearbeatInterval = $this->config['options']['heartbeat'] ?? 0;
        if (!$hearbeatInterval) {
            return;
        }

        if (function_exists('\go')) {
            \go(function () use ($hearbeatInterval) {
                while (true) {
                    \Co::sleep($hearbeatInterval);
                    $this->channel->getConnection()->checkHeartBeat();
                }
            });
        }
    }
}
