<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Unit\Console;

use Carbon\Carbon;
use GuzzleHttp\Client;
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Middleware;
use GuzzleHttp\Psr7\Response;
use Illuminate\Cache\ArrayStore;
use Illuminate\Cache\Repository;
use Orchestra\Testbench\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Output\BufferedOutput;
use VladimirYuldashev\LaravelQueueRabbitMQ\Console\GarbageCollector;

class GarbageCollectorTest extends TestCase
{
    private const T0 = '2026-10-01 10:00:00';

    private const ORG_QUEUE = 'qa-micro-integration.hubspot.contacts_sync.organization.60509';

    private array $history = [];

    private ?MockHandler $mock = null;

    private Repository $cache;

    private array $config = [
        'secure' => false,
        'hosts' => [['api_host' => 'rabbit.test', 'api_port' => 15672, 'user' => 'u', 'password' => 'p']],
    ];

    protected function setUp(): void
    {
        parent::setUp();

        $this->cache = new Repository(new ArrayStore);
        Carbon::setTestNow(self::T0);
    }

    protected function tearDown(): void
    {
        Carbon::setTestNow();

        parent::tearDown();
    }

    private static function q(string $name, int $messages = 0, string $type = 'quorum', array $arguments = []): array
    {
        return ['name' => $name, 'type' => $type, 'messages' => $messages, 'arguments' => (object) $arguments];
    }

    private static function ok(array|object $body): Response
    {
        return new Response(200, [], json_encode($body));
    }

    private static function details(string $name, int $messages = 0, int $consumers = 0, ?int $publish = 0, string $type = 'quorum'): Response
    {
        $body = ['name' => $name, 'type' => $type, 'messages' => $messages, 'consumers' => $consumers];
        if ($publish !== null) {
            $body['message_stats'] = ['publish' => $publish];
        }

        return self::ok($body);
    }

    /**
     * Runs the command once with the queued responses and returns the requests it made as "METHOD path".
     */
    private function run_gc(array $responses, ?array $config = null): array
    {
        $this->history = [];
        $this->mock = new MockHandler($responses);
        $stack = HandlerStack::create($this->mock);
        $stack->push(Middleware::history($this->history));

        $command = new GarbageCollector($config ?? $this->config, new Client(['handler' => $stack]), $this->cache);
        $command->setLaravel($this->app);
        $command->run(new ArrayInput([]), new BufferedOutput);

        return array_map(
            fn (array $t) => $t['request']->getMethod().' '.$t['request']->getUri()->getPath()
                .($t['request']->getUri()->getQuery() ? '?'.$t['request']->getUri()->getQuery() : ''),
            $this->history
        );
    }

    private function deletes(array $requests): array
    {
        return array_values(array_filter($requests, fn (string $r) => str_starts_with($r, 'DELETE')));
    }

    private function advance(int $seconds): void
    {
        Carbon::setTestNow(Carbon::now()->addSeconds($seconds));
    }

    public function test_never_deletes_delay_and_system_queues_even_when_idle_for_ever(): void
    {
        $list = self::ok([
            self::q('delay.10000'),
            self::q('delay.30.something.organization.1', 0, 'classic'),
            self::q('notes.delay.10000'),
            self::q('qa.shaped.organization.2', 0, 'quorum', ['x-message-ttl' => 5000, 'x-dead-letter-exchange' => '']),
            self::q('default'),
            self::q('failed.qa.organization.3'),
            self::q('qa.dlq.organization.4'),
        ]);

        foreach ([0, 86400, 86400 * 3] as $step) {
            $this->advance($step);
            $requests = $this->run_gc([$list]);
            $this->assertSame(['GET /api/queues/%2F'], array_map(fn ($r) => explode('?', $r)[0], $requests));
        }
    }

    public function test_queue_outside_the_allowlist_is_never_deleted(): void
    {
        $list = self::ok([self::q('production-notes-prepare-quorum')]);

        $this->run_gc([$list]);
        $this->advance(86400 * 2);
        $requests = $this->run_gc([$list]);

        $this->assertSame([], $this->deletes($requests));
        $this->assertCount(1, $requests, 'no per-queue requests for a non-allowlisted queue');
    }

    public function test_idle_queue_is_deleted_only_after_the_grace_period(): void
    {
        $list = [self::ok([self::q(self::ORG_QUEUE)])];
        $path = '/api/queues/%2F/'.self::ORG_QUEUE;

        // first sight: records state, no delete
        $requests = $this->run_gc([...$list, self::details(self::ORG_QUEUE, 0, 0, 7)]);
        $this->assertSame([], $this->deletes($requests));
        $this->assertContains('GET '.$path, $requests);

        // 23 h later: still within grace, no per-queue request and no delete
        $this->advance(23 * 3600);
        $requests = $this->run_gc($list);
        $this->assertCount(1, $requests);

        // 24 h after first sight: re-check, then delete (quorum -> no if-* flags)
        $this->advance(3600);
        $requests = $this->run_gc([...$list, self::details(self::ORG_QUEUE, 0, 0, 7), new Response(204)]);
        $this->assertSame(['GET /api/queues/%2F', 'GET '.$path, 'DELETE '.$path], array_map(fn ($r) => explode('?', $r)[0], $requests));
        $this->assertSame('DELETE '.$path, $requests[2]);

        // state was cleared: a recreated queue starts from scratch
        $requests = $this->run_gc([...$list, self::details(self::ORG_QUEUE, 0, 0, 0)]);
        $this->assertSame([], $this->deletes($requests));
    }

    public function test_classic_queue_is_deleted_with_if_empty_and_if_unused(): void
    {
        $name = 'qa.classic.organization.9';
        $list = [self::ok([self::q($name, 0, 'classic')])];

        $this->run_gc([...$list, self::details($name, 0, 0, 1, 'classic')]);
        $this->advance(86400);
        $requests = $this->run_gc([...$list, self::details($name, 0, 0, 1, 'classic'), new Response(204)]);

        $this->assertSame(['DELETE /api/queues/%2F/'.$name.'?if-empty=true&if-unused=true'], $this->deletes($requests));
    }

    public function test_new_publish_since_first_sight_resets_the_timer(): void
    {
        $list = [self::ok([self::q(self::ORG_QUEUE)])];

        $this->run_gc([...$list, self::details(self::ORG_QUEUE, 0, 0, 5)]);
        $this->advance(86400);
        // empty now, but 3 more messages were published (and drained) in between
        $requests = $this->run_gc([...$list, self::details(self::ORG_QUEUE, 0, 0, 8)]);
        $this->assertSame([], $this->deletes($requests));

        // timer restarted at this run: not due after another 23 h
        $this->advance(23 * 3600);
        $this->assertCount(1, $this->run_gc($list));

        // idle for a full grace period since the reset: deleted
        $this->advance(3600);
        $requests = $this->run_gc([...$list, self::details(self::ORG_QUEUE, 0, 0, 8), new Response(204)]);
        $this->assertCount(1, $this->deletes($requests));
    }

    public function test_message_seen_in_the_list_resets_the_timer(): void
    {
        $empty = self::ok([self::q(self::ORG_QUEUE)]);
        $busy = self::ok([self::q(self::ORG_QUEUE, 4)]);

        $this->run_gc([$empty, self::details(self::ORG_QUEUE, 0, 0, 1)]);
        $this->advance(12 * 3600);
        $this->run_gc([$busy]);
        $this->advance(12 * 3600);
        // 24 h since the first sight, but the queue was busy in between: starts over
        $requests = $this->run_gc([$empty, self::details(self::ORG_QUEUE, 0, 0, 1)]);
        $this->assertSame([], $this->deletes($requests));
    }

    public function test_queue_with_a_consumer_is_not_deleted(): void
    {
        $list = [self::ok([self::q(self::ORG_QUEUE)])];

        $this->run_gc([...$list, self::details(self::ORG_QUEUE, 0, 0, 1)]);
        $this->advance(86400);
        $requests = $this->run_gc([...$list, self::details(self::ORG_QUEUE, 0, 1, 1)]);

        $this->assertSame([], $this->deletes($requests));
    }

    public function test_queue_that_got_a_message_between_list_and_delete_is_not_deleted(): void
    {
        $list = [self::ok([self::q(self::ORG_QUEUE)])];

        $this->run_gc([...$list, self::details(self::ORG_QUEUE, 0, 0, 1)]);
        $this->advance(86400);
        // the list said empty, the per-queue re-check right before the delete says 1 message
        $requests = $this->run_gc([...$list, self::details(self::ORG_QUEUE, 1, 0, 1)]);

        $this->assertSame([], $this->deletes($requests));
    }

    public function test_dead_letter_target_of_a_non_empty_queue_is_still_protected(): void
    {
        $list = [self::ok([
            self::q(self::ORG_QUEUE),
            self::q('delay.3.'.self::ORG_QUEUE, 2, 'classic', ['x-dead-letter-routing-key' => self::ORG_QUEUE, 'x-dead-letter-exchange' => '']),
        ])];

        $this->run_gc($list);
        $this->advance(86400 * 2);
        $requests = $this->run_gc($list);

        $this->assertSame([], $this->deletes($requests));
        $this->assertCount(1, $requests);
    }

    public function test_the_list_is_fetched_once_on_success_and_nothing_is_deleted_when_every_attempt_fails(): void
    {
        $requests = $this->run_gc([self::ok([self::q('delay.1000')])]);
        $this->assertCount(1, $requests, 'one list request when the first attempt succeeds');

        $failures = array_fill(0, 5, new Response(500));
        $requests = $this->run_gc($failures);
        $this->assertCount(5, $requests);
        $this->assertSame([], $this->deletes($requests));
    }

    public function test_list_fetch_is_retried_after_a_failure(): void
    {
        $requests = $this->run_gc([new Response(500), self::ok([self::q('delay.1000')])]);

        $this->assertCount(2, $requests);
    }

    public function test_queue_names_are_url_encoded(): void
    {
        $name = 'qa weird/name.organization.5';
        $list = [self::ok([self::q($name)])];

        $requests = $this->run_gc([...$list, self::details($name, 0, 0, 0)]);

        $this->assertContains('GET /api/queues/%2F/qa%20weird%2Fname.organization.5', $requests);
    }

    public function test_allowlist_and_grace_period_are_configurable(): void
    {
        $config = $this->config + ['garbage_collector' => ['allowlist' => ['/^tmp\./'], 'grace_seconds' => 600]];
        $list = [self::ok([self::q('tmp.reports'), self::q(self::ORG_QUEUE)])];

        $this->run_gc([...$list, self::details('tmp.reports', 0, 0, 0)], $config);
        $this->advance(600);
        $requests = $this->run_gc([...$list, self::details('tmp.reports', 0, 0, 0), new Response(204)], $config);

        $this->assertSame(['DELETE /api/queues/%2F/tmp.reports'], $this->deletes($requests));
    }
}
