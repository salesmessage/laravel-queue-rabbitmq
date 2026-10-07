<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Unit\Queue;

use PHPUnit\Framework\TestCase;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\QueueConfig;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\QueueConfigFactory;

class QueueConfigFactoryTest extends TestCase
{
    public function test_delay_cap_defaults_to_one_day_in_log_mode(): void
    {
        $config = QueueConfigFactory::make();

        $this->assertSame(86400, $config->getMaxDelaySeconds());
        $this->assertSame(QueueConfig::MAX_DELAY_MODE_LOG, $config->getMaxDelayMode());
    }

    public function test_delay_cap_is_read_from_queue_options_and_not_kept_as_an_extra_option(): void
    {
        $config = QueueConfigFactory::make(['options' => ['queue' => [
            'max_delay_seconds' => '3600',
            'max_delay_mode' => 'throw',
        ]]]);

        $this->assertSame(3600, $config->getMaxDelaySeconds());
        $this->assertSame(QueueConfig::MAX_DELAY_MODE_THROW, $config->getMaxDelayMode());
        $this->assertSame([], $config->getOptions());
    }

    public function test_empty_max_delay_seconds_keeps_the_default(): void
    {
        $config = QueueConfigFactory::make(['options' => ['queue' => ['max_delay_seconds' => '']]]);

        $this->assertSame(86400, $config->getMaxDelaySeconds());
    }

    public function test_zero_max_delay_seconds_is_kept(): void
    {
        $config = QueueConfigFactory::make(['options' => ['queue' => ['max_delay_seconds' => '0']]]);

        $this->assertSame(0, $config->getMaxDelaySeconds());
    }
}
