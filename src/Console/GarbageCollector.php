<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Console;

use Carbon\Carbon;
use GuzzleHttp\Client;
use Illuminate\Console\Command;

class GarbageCollector extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'rabbitmq:garbage';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Removes unused rabbitmq queues';

    /** @var array */
    protected array $config;

    /**
     *
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function handle()
    {
        $scheme = $this->config['secure'] ? 'https://' : 'http://';
        $host = $this->config['hosts'][0]['host'];
        $port = $this->config['hosts'][0]['api_port'];
        $username = $this->config['hosts'][0]['user'];
        $password = $this->config['hosts'][0]['password'];
        $client = new Client();
        $url = $host . ':' . $port;
        $res = $client->get(
            "{$scheme}{$url}/api/queues",
            [
                'headers' => [
                    'Authorization' => 'Basic ' . base64_encode(
                            $username . ':' . $password
                        )
                ]
            ]
        );
        $queues = json_decode($res->getBody());
        $queuesToRemove = collect($queues)
            ->filter(function ($queue) {
                $messages = $queue->messages ?? 0;
                return $queue->name !== 'default' && !str_contains(
                        $queue->name,
                        'failed'
                    ) && !str_contains(
                        $queue->name,
                        'dlq'
                    ) && $messages === 0 && ($queue->messages_details?->rate ?? 0) === 0.0
                    && $queue->messages_ready_details->rate === 0.0
                    && $queue->messages_unacknowledged_details->rate === 0.0;
            })
            ->pluck('name')
            ->values()
            ->toArray();

        foreach ($queuesToRemove as $queue) {
            try {
                $client->delete(
                    "{$scheme}$url/api/queues/%2F/{$queue}?if-empty=true&if-unused=true", // %2F stands for /
                    [
                        'headers' => [
                            'Authorization' => 'Basic ' . base64_encode(
                                    $username . ':' . $password
                                )
                        ]
                    ]
                );
                $this->info("RabbitMQ. Delete $queue queue");
            } catch (\Throwable $exception) {
                $this->warn("Was not able to remove $queue with error {$exception->getMessage()}");
                logger()->warning('RabbitMQ Garbage Collector failed to remove queue', [
                    'queue' => $queue,
                    'message' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString()
                ]);
            }
        }

        $this->info('Garbage collector finished');
    }
}
