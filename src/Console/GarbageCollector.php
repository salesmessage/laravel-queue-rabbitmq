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

    /**
     * Execute the console command.
     *
     * @return mixed
     * @throws \GuzzleHttp\Exception\GuzzleException
     */
    public function handle()
    {
        $host = Illuminate\Support\Facades\Config::get('queue.connections.rabbitmq.hosts')[0]['host'];
        $port = Illuminate\Support\Facades\Config::get('queue.connections.rabbitmq.hosts')[0]['api_port'];
        $username = Illuminate\Support\Facades\Config::get('queue.connections.rabbitmq.hosts')[0]['user'];
        $password = Illuminate\Support\Facades\Config::get('queue.connections.rabbitmq.hosts')[0]['password'];
        $client = new Client();
        $url = $host . ':' . $port;
        $res = $client->get(
            "$url/api/queues",
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
                return $queue->name !== 'default' && !str_contains($queue->name, 'failed.') && $messages === 0 && $queue->messages_details->rate === 0.0 && $queue->messages_ready_details->rate === 0.0 && $queue->messages_unacknowledged_details->rate === 0.0;
            })
            ->pluck('name')
            ->values()
            ->toArray();

        foreach ($queuesToRemove as $queue)
        {
            try {
                $client->delete(
                    "$url/api/queues/%2F/$queue", // %2F stands for /
                    [
                        'headers' => [
                            'Authorization' => 'Basic ' . base64_encode(
                                    $username . ':' . $password
                                )
                        ]
                    ]
                );
                $this->info("Delete $queue queue");
            } catch (\Throwable $exception) {
                $this->warn("Was not able to remove $queue with error {$exception->getMessage()}");
                logger()->error('RabbitMQ Garbage Collector failed to remove queue', [
                    'queue' => $queue,
                    'message' => $exception->getMessage(),
                    'trace' => $exception->getTraceAsString()
                ]);
            }
        }

        $this->info('Garbage collector finished');
    }
}