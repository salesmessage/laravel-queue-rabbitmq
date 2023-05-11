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

        $this->channel->basic_qos(
            $this->prefetchSize,
            $this->prefetchCount,
            true
        );

        $arguments = [];
        if ($this->maxPriority) {
            $arguments['priority'] = ['I', $this->maxPriority];
        }

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
                $this->channel->wait(null, false, (int)$options->timeout);
            } catch (AMQPRuntimeException $exception) {
                $this->exceptions->report($exception);

                $this->kill(self::EXIT_ERROR, $options);
            } catch (AMQPTimeoutException) {
                if ($this->prefetchCount > 1) {
                    logger()->info('Timeout reached. Processing what is left', [
                        'timeout' => (int)$options->timeout,
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
        $this->discoverQueues();
        if ($this->roundRobin) {
            $this->switchToNextQueue(true);
        } else {
            foreach ($this->currentQueues as $queue)
            {
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
            if ($this->preCheck) {
                $client = new Client();

                $host = $this->config['hosts'][0]['host'];
                $port = $this->config['hosts'][0]['api_port'];
                $username = $this->config['hosts'][0]['user'];
                $password = $this->config['hosts'][0]['password'];

                $url = $host . ':' . $port;
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
                if ($queueData->messages_ready === 0 || (
                        ($queueData->arguments->{'x-single-active-consumer'} ?? false) === true
                        && $queueData->consumers > 0
                    )) {
                    logger()->info('Precheck failed for queue', [
                        'queue' => $nextQueue,
                        'workerName' => $this->name,
                        'single-active' => $queueData->arguments->{'x-single-active-consumer'} ?? false,
                        'active-consumers' => $queueData->consumers,
                    ]);
                }
            }
        } while ($this->preCheck && ($queueData->messages_ready ?? 0) === 0);

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
        if ($this->processedJob >= $this->prefetchCount) {
            logger()->info('Processing batch', [
                'workerName' => $this->name,
                'batchCount' => count($this->currentMessages),
            ]);
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
        logger()->info('Processing single', [
            'workerName' => $this->name,
            'messageBody' => $message->getBody(),
        ]);
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
            if ($this->prefetchCount > 1) {
                $this->batchHandler($message);
            } else {
                $this->singleHandler($message);
            }
        };

        logger()->info('Started listening to queue ' . $queue, [
            'queue' => $queue,
            'workerName' => $this->name
        ]);

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
        logger()->info('Stop listening to queue', [
            'currentConsumer' => $this->consumerTag,
        ]);
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
            $url = $host . ':' . $port;
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
                    return str_starts_with($queue->name, $this->mask) && (($this->roundRobin && $readyMessages > 0) || !$this->roundRobin);
                })
                ->pluck('name')
                ->values()
                ->toArray();

            $this->currentQueues = $queues;
            if (count($this->currentQueues) === 0) {
                logger()->info('No queues found', [
                    'workerName' => $this->name
                ]);
                $this->sleep($this->options->sleep);
            } else {
                logger()->info('Discovered queues', [
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
            logger()->warning('No jobs to process', [
                'workerName' => $this->name,
            ]);
            return;
        }

        logger()->warning('Processing jobs', [
            'workerName' => $this->name,
            'jobsCount' => count($this->currentMessages)
        ]);

        $class = $this->getJobByMessage($this->currentMessages[0])->getPayloadClass();
        $sameClass = collect($this->currentMessages)->every(function (AMQPMessage $message) use ($class) {
            return $this->getJobByMessage($message)->getPayloadClass() === $class;
        });

        $reflection = new \ReflectionClass($class);

        if (count($this->currentMessages) > 1 && $sameClass && $reflection->implementsInterface(RabbitMQBatchable::class)) {
            $batchData = [];
            foreach ($this->currentMessages as $message) {
                $job = $this->getJobByMessage($message);
                $batchData[] = $job->getPayloadData();
            }

            $class::collection($batchData);
            $this->processed += count($this->currentMessages);
            return;
        }

        /** @var AMQPMessage $message */
        foreach ($this->currentMessages as $message) {
            $this->processMessage($message);
        }
        $this->processed += count($this->currentMessages);
    }
}
