<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Console;

use Carbon\Carbon;
use GuzzleHttp\Client;
use Illuminate\Console\Command;
use Illuminate\Contracts\Cache\Repository as CacheRepository;

/**
 * Removes abandoned dynamic queues from the main vhost ("/") only.
 *
 * A queue is deleted only when ALL of the following hold:
 *  - its name matches the allowlist (default: per-organization queues, "*.organization.<id>");
 *  - it is not a delay queue, a failed/dlq queue, or the dead-letter target of a non-empty queue;
 *  - it has been seen empty, without consumers and without any new publish for the whole grace period
 *    (24 h by default), tracked in the cache between runs;
 *  - a fresh per-queue check made right before the delete confirms it is still empty and has no consumers.
 *
 * Optional connection config keys (queue.connections.rabbitmq):
 *  - garbage_collector.allowlist: array of regular expressions matched against the queue name;
 *  - garbage_collector.grace_seconds: idle time required before a queue is deleted.
 */
class GarbageCollector extends Command
{
    private const STATE_CACHE_KEY = 'rabbitmq:garbage:idle_queues';

    private const DEFAULT_GRACE_SECONDS = 86400;

    private const DEFAULT_ALLOWLIST = ['/\.organization\.\d+$/'];

    private const FETCH_ATTEMPTS = 5;

    protected $signature = 'rabbitmq:garbage';

    protected $description = 'Removes unused rabbitmq queues';

    protected array $config;

    private Client $client;

    private ?CacheRepository $cache;

    public function __construct(array $config, ?Client $client = null, ?CacheRepository $cache = null)
    {
        $this->config = $config;
        $this->client = $client ?? new Client;
        $this->cache = $cache;
        parent::__construct();
    }

    public function handle()
    {
        $queues = $this->fetchQueues();
        if ($queues === null) {
            $this->warn('Was not able to get the queues list, nothing was removed');
            logger()->warning('RabbitMQ Garbage Collector could not get the queues list, nothing was removed');

            return;
        }

        $now = Carbon::now()->getTimestamp();
        $graceSeconds = $this->graceSeconds();
        $protected = $this->deadLetterTargets($queues);
        $state = $this->loadState();
        $newState = [];
        $skipped = [];
        $candidates = 0;

        foreach ($queues as $queue) {
            $name = (string) $queue->name;

            if (null !== ($reason = $this->skipReason($queue, $protected))) {
                $skipped[$reason] = ($skipped[$reason] ?? 0) + 1;
                logger()->debug('RabbitMQ Garbage Collector skipped queue', ['queue' => $name, 'reason' => $reason]);

                continue;
            }

            // Not empty: it is in use, start over.
            if (($queue->messages ?? 0) > 0) {
                logger()->debug('RabbitMQ Garbage Collector skipped queue', ['queue' => $name, 'reason' => 'has_messages']);

                continue;
            }

            $candidates++;
            $entry = $state[$name] ?? null;

            if ($entry === null) {
                // First time seen idle: remember when and how many messages were published so far.
                $details = $this->fetchQueue($name);
                if ($details !== null) {
                    $newState[$name] = ['since' => $now, 'publish' => $this->publishedCount($details)];
                }

                continue;
            }

            if ($now - (int) $entry['since'] < $graceSeconds) {
                $newState[$name] = $entry;

                continue;
            }

            // Due: re-check the queue itself right before the delete.
            $details = $this->fetchQueue($name);
            if ($details === null) {
                $newState[$name] = $entry;

                continue;
            }

            $publish = $this->publishedCount($details);
            if (($details->messages ?? 0) > 0
                || ($details->consumers ?? 0) > 0
                || $publish !== $entry['publish']) {
                // Used since it was first seen (or being used right now): start over.
                $newState[$name] = ['since' => $now, 'publish' => $publish];
                logger()->debug('RabbitMQ Garbage Collector reset idle timer', ['queue' => $name]);

                continue;
            }

            if ($this->deleteQueue($name, (string) ($details->type ?? ''))) {
                logger()->info('RabbitMQ Garbage Collector deleted queue', [
                    'queue' => $name,
                    'type' => $details->type ?? null,
                    'idle_since' => Carbon::createFromTimestamp((int) $entry['since'])->toIso8601String(),
                    'published_total' => $publish,
                    'grace_seconds' => $graceSeconds,
                ]);

                continue;
            }

            $newState[$name] = $entry;
        }

        $this->saveState($newState, $graceSeconds);

        logger()->info('RabbitMQ Garbage Collector loaded queues filtered', [
            'queues_count' => count($queues),
            'queues_filtered' => $candidates,
            'queues_skipped' => $skipped,
        ]);

        $this->info('Garbage collector finished');
    }

    /**
     * The whole list is fetched once; null when every attempt failed.
     */
    private function fetchQueues(): ?array
    {
        for ($try = 1; $try <= self::FETCH_ATTEMPTS; $try++) {
            try {
                $response = $this->client->get(
                    $this->baseUrl().'/api/queues/%2F?disable_stats=true&enable_queue_totals=true',
                    ['headers' => $this->headers()]
                );
                $queues = json_decode($response->getBody());

                if (is_array($queues)) {
                    return $queues;
                }
            } catch (\Throwable $exception) {
                logger()->warning('RabbitMQ Garbage Collector failed to get queues', [
                    'message' => $exception->getMessage(),
                ]);
            }
        }

        return null;
    }

    /**
     * A single queue with its stats (consumers, message_stats), unlike the list which is requested
     * without stats to stay cheap.
     */
    private function fetchQueue(string $name): ?object
    {
        try {
            $response = $this->client->get(
                $this->baseUrl().'/api/queues/%2F/'.rawurlencode($name),
                ['headers' => $this->headers()]
            );
            $queue = json_decode($response->getBody());

            return is_object($queue) ? $queue : null;
        } catch (\Throwable $exception) {
            logger()->warning('RabbitMQ Garbage Collector failed to get queue', [
                'queue' => $name,
                'message' => $exception->getMessage(),
            ]);

            return null;
        }
    }

    private function deleteQueue(string $name, string $type): bool
    {
        // RabbitMQ rejects if-empty/if-unused on quorum queues entirely
        // (see https://github.com/rabbitmq/rabbitmq-server/issues/10543), so a plain delete is used for them;
        // emptiness was checked right before.
        $deleteQuery = $type === 'quorum' ? '' : '?if-empty=true&if-unused=true';

        try {
            $this->client->delete(
                $this->baseUrl().'/api/queues/%2F/'.rawurlencode($name).$deleteQuery,
                ['headers' => $this->headers()]
            );
            $this->info("RabbitMQ. Delete $name queue");

            return true;
        } catch (\Throwable $exception) {
            $this->warn("Was not able to remove $name with error {$exception->getMessage()}");
            logger()->warning('RabbitMQ Garbage Collector failed to remove queue', [
                'queue' => $name,
                'message' => $exception->getMessage(),
                'trace' => $exception->getTraceAsString(),
            ]);

            return false;
        }
    }

    /**
     * Why a queue must never be deleted, or null when it may be a candidate.
     */
    private function skipReason(object $queue, array $protected): ?string
    {
        $name = (string) $queue->name;
        $arguments = (array) ($queue->arguments ?? []);

        if ($name === 'default') {
            return 'default';
        }

        if (str_contains($name, 'failed') || str_contains($name, 'dlq')) {
            return 'failed_or_dlq';
        }

        // delay.<ms> (php-lib-rabbitmq), delay.<ttl>.<queue> (this library) and legacy <queue>.delay.<ms>
        // queues fill and empty constantly and must never be removed from here.
        if (str_starts_with($name, 'delay.') || str_contains($name, '.delay.')) {
            return 'delay_queue';
        }

        if (isset($arguments['x-message-ttl'], $arguments['x-dead-letter-exchange'])) {
            return 'delay_queue';
        }

        if (isset($protected[$name])) {
            return 'dead_letter_target';
        }

        if (! $this->isAllowlisted($name)) {
            return 'not_allowlisted';
        }

        return null;
    }

    private function isAllowlisted(string $name): bool
    {
        $patterns = $this->config['garbage_collector']['allowlist'] ?? self::DEFAULT_ALLOWLIST;

        foreach ($patterns as $pattern) {
            if (preg_match($pattern, $name) === 1) {
                return true;
            }
        }

        return false;
    }

    /**
     * Queues that are the dead-letter routing key target of a non-empty queue.
     */
    private function deadLetterTargets(array $queues): array
    {
        $targets = [];

        foreach ($queues as $queue) {
            $arguments = (array) ($queue->arguments ?? []);
            $dlx = $arguments['x-dead-letter-exchange'] ?? null;
            $dlk = $arguments['x-dead-letter-routing-key'] ?? null;

            if (empty($dlk) || 0 === ($queue->messages ?? 0)) {
                continue;
            }

            $targets[$dlk] = ! empty($dlx) ? $dlx : $dlk;
        }

        return $targets;
    }

    private function publishedCount(object $queue): int
    {
        return (int) ($queue->message_stats->publish ?? 0);
    }

    private function graceSeconds(): int
    {
        return (int) ($this->config['garbage_collector']['grace_seconds'] ?? self::DEFAULT_GRACE_SECONDS);
    }

    private function loadState(): array
    {
        $state = $this->cache()->get(self::STATE_CACHE_KEY);

        return is_array($state) ? $state : [];
    }

    private function saveState(array $state, int $graceSeconds): void
    {
        // Entries expire on their own long after the grace period if the command stops running.
        $this->cache()->put(self::STATE_CACHE_KEY, $state, max($graceSeconds * 3, 3600));
    }

    private function cache(): CacheRepository
    {
        return $this->cache ??= $this->laravel['cache.store'];
    }

    private function baseUrl(): string
    {
        $scheme = $this->config['secure'] ? 'https://' : 'http://';

        return $scheme.$this->config['hosts'][0]['api_host'].':'.$this->config['hosts'][0]['api_port'];
    }

    private function headers(): array
    {
        return [
            'Authorization' => 'Basic '.base64_encode(
                $this->config['hosts'][0]['user'].':'.$this->config['hosts'][0]['password']
            ),
        ];
    }
}
