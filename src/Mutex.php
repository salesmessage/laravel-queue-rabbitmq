<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ;

use VladimirYuldashev\LaravelQueueRabbitMQ\Exceptions\MutexTimeout;

final class Mutex
{
    private const DEFAULT_WAITING_CHANNEL_TIMEOUT = 60;

    private ?string $currentLockInitiator = null;
    private $availableLocksPool = null;

    public function __construct(bool $asyncMode = true)
    {
        if (!$asyncMode) {
            return;
        }

        if (extension_loaded('swoole')) {
            $this->availableLocksPool = new \Swoole\Coroutine\Channel();
        } elseif (extension_loaded('openswoole')) {
            $this->availableLocksPool = new \OpenSwoole\Coroutine\Channel();
        }

        // initialize pool with 1 available lock
        $this->availableLocksPool?->push(true);
    }

    public function unlock(string $initiator): void
    {
        if ($this->availableLocksPool === null) {
            return;
        }

        if ($this->currentLockInitiator !== $initiator) {
            return;
        }
        $this->currentLockInitiator = null;
        if (!$this->hasAvailableLock()) {
            $this->availableLocksPool->push(true);
        }
    }

    public function lock(string $initiator, float $timeout = null): void
    {
        if ($this->availableLocksPool === null) {
            return;
        }

        // if the same initiator tries to lock, we allow it and ignore
        if (!$this->hasAvailableLock() && $this->currentLockInitiator === $initiator) {
            return;
        }

        $hasAvailableLock = $this->availableLocksPool->pop($timeout ?: self::DEFAULT_WAITING_CHANNEL_TIMEOUT);
        if (!$hasAvailableLock) {
            throw new MutexTimeout('Mutex error on trying to acquire lock');
        }
        $this->currentLockInitiator = $initiator;
    }

    public function hasAvailableLock(): bool
    {
        return !$this->availableLocksPool?->isEmpty();
    }
}
