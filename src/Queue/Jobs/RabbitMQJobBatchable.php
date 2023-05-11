<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs;

use Illuminate\Contracts\Encryption\Encrypter;
use Illuminate\Queue\Jobs\JobName;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob as BaseJob;

/**
 * SQS Job wrapper for RabbitMQ
 */
class RabbitMQJobBatchable extends BaseJob
{
    /**
     * Fire the job.
     *
     * @return void
     */
    public function fire()
    {
        $payload = $this->payload();

        [$class, $method] = JobName::parse($payload['job']);

        ($this->instance = $this->resolve($class))->{$method}($this, $payload['data']);
    }

    /**
     * Returns target class name
     *
     * @return mixed
     */
    public function getPayloadClass(): string
    {
        $payload = $this->payload();

        return $payload['data']['commandName'];
    }

    /**
     * @return object
     * @throws \RuntimeException
     */
    public function getPayloadData(): object
    {
        $payload = $this->payload();

        $data = $payload['data'];

        if (str_starts_with($data['command'], 'O:')) {
            return unserialize($data['command']);
        }

        if ($this->container->bound(Encrypter::class)) {
            return unserialize($this->container[Encrypter::class]->decrypt($data['command']));
        }

        throw new \RuntimeException('Unable to extract job data.');
    }
}
