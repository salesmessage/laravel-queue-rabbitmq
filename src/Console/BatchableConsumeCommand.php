<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Console;

use Illuminate\Queue\Console\WorkCommand;
use Illuminate\Support\Str;
use Symfony\Component\Console\Terminal;
use VladimirYuldashev\LaravelQueueRabbitMQ\BatchableConsumer;

class BatchableConsumeCommand extends WorkCommand
{
    protected $signature = 'rabbitmq:work
                            {connection? : The name of the queue connection to work}
                            {--name=default : The name of the consumer}
                            {--queue= : The names of the queues to work}
                            {--once : Only process the next job on the queue}
                            {--stop-when-empty : Stop when the queue is empty}
                            {--delay=0 : The number of seconds to delay failed jobs (Deprecated)}
                            {--backoff=0 : The number of seconds to wait before retrying a job that encountered an uncaught exception}
                            {--max-jobs=0 : The number of jobs to process before stopping}
                            {--max-time=0 : The maximum number of seconds the worker should run}
                            {--force : Force the worker to run even in maintenance mode}
                            {--memory=128 : The memory limit in megabytes}
                            {--sleep=3 : Number of seconds to sleep when no job is available}
                            {--timeout=60 : The number of seconds a child process can run}
                            {--consume-interval=60 : Time interval in sec when consumer should check queues and then handle messages}
                            {--consume-interval-mapping=false : Enabled prefetch adjusting depending on queue. Example "20:500;40:1000" }
                            {--tries=1 : Number of times to attempt a job before logging it failed}
                            {--rest=0 : Number of seconds to rest between jobs}
                            {--precheck=1 : Runs precheck before switching to queue}
                            {--auto-prefetch=1 : Enabled prefetch adjusting depending on queue}
                            {--roundrobin=1 : Consumer goes between queues one-by-one(round-robin style)}
                            {--async-mode=0 : Async processing for some functionality (now only "heartbeat" is supported)}

                            {--max-priority=}
                            {--consumer-tag}
                            {--prefetch-size=0}
                            {--prefetch-count=1}
                            {--mask=default}
                           ';

    protected $description = 'Consume messages';

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        /** @var BatchableConsumer $consumer */
        $consumer = app(BatchableConsumer::class);

        $consumer->setContainer($this->laravel);
        $consumer->setName($this->option('name'));
        $consumer->setConsumerTag($this->consumerTag());
        $consumer->setMaxPriority((int) $this->option('max-priority'));
        $consumer->setPrefetchSize((int) $this->option('prefetch-size'));
        $consumer->setPrefetchCount((int) $this->option('prefetch-count'));
        $consumer->setMask((string) $this->option('mask'));
        $consumer->setPreCheck((bool) $this->option('precheck'));
        $consumer->setRoundRobin((bool) $this->option('roundrobin'));
        $consumer->setAutoPrefetch((bool) $this->option('auto-prefetch'));
        $consumer->setAsyncMode((bool) $this->option('async-mode'));
        $consumer->setConsumeInterval((int) $this->option('consume-interval'));

        $consumeIntervalMapping = $this->option('consume-interval-mapping');
        if ($consumeIntervalMapping !== 'false') {
            $intervals = explode(';', $consumeIntervalMapping);
            foreach ($intervals as &$interval) {
                $values = explode(':', $interval);
                $interval = [
                    'range' => $values[0],
                    'interval' => $values[1],
                ];
            }
            $intervals = collect($intervals)->sortBy('range')->values()->toArray();
            $consumer->setConsumeIntervalMapping($intervals);
        }

        if ($this->downForMaintenance() && $this->option('once')) {
            $this->worker->sleep($this->option('sleep'));
            return 0;
        }

        $this->listenForEvents();

        $connection = $this->argument('connection')
            ?: $this->laravel['config']['queue.default'];

        $queue = $this->getQueue($connection);

        if (Terminal::hasSttyAvailable()) {
            $this->components->info(
                sprintf('Processing jobs using the [%s] %s.', $consumer->getMask(), str('mask')->plural(explode(',', $queue)))
            );
        }

        return $this->runWorker(
            $connection, $queue
        );
    }

    /**
     * @return string
     */
    protected function consumerTag(): string
    {
        if ($consumerTag = $this->option('consumer-tag')) {
            return $consumerTag;
        }

        $consumerTag = implode('_', [
            Str::slug(config('app.name', 'laravel')),
            Str::slug($this->option('name')),
            md5(serialize($this->options()).Str::random(16).getmypid()),
        ]);

        return Str::substr($consumerTag, 0, 255);
    }
}
