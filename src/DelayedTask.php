<?php

namespace Sevming\DelayedTask;

use RedisException, Redis;

class DelayedTask
{
    /**
     * @var string
     */
    public $prefix = 'dt_';

    /**
     * Get the number of delayed tasks each time.
     *
     * @var int
     */
    public $bucketRangeLimit = 1;

    /**
     * Number of bucket processes.(Number of storage bucket.)
     *
     * @var int
     */
    public $bucketCount = 4;

    /**
     * Number of queue processes.
     *
     * @var int
     */
    public $queueCount = 4;

    /**
     * Redis config.
     *
     * @var array
     */
    protected $config = [];

    /**
     * Redis instance.
     *
     * @var null
     */
    protected static $redis = null;

    /**
     * Number of task processing corresponding to queue priority.
     *
     * @var array
     */
    protected $queueTaskPriority = [
        self::QUEUE_PRIORITY_1 => 2,
        self::QUEUE_PRIORITY_2 => 3,
        self::QUEUE_PRIORITY_3 => 5
    ];

    /**
     * @var int
     */
    protected const QUEUE_PRIORITY_1 = 1;
    protected const QUEUE_PRIORITY_2 = 2;
    protected const QUEUE_PRIORITY_3 = 3;

    /**
     * Task status delay.
     *
     * @var int
     */
    protected const TASK_STATUS_DELAY = 1;

    /**
     * Task status ok.
     *
     * @var int
     */
    protected const TASK_STATUS_OK = 2;

    /**
     * Task status fail.
     *
     * @var int
     */
    protected const TASK_STATUS_FAIL = 3;

    /**
     * Task status deleted.
     *
     * @var int
     */
    protected const TASK_STATUS_DELETED = 4;

    /**
     * Constructor.
     *
     * @param string $host
     * @param int    $port
     * @param string $auth
     */
    public function __construct(string $host = '127.0.0.1', int $port = 6379, string $auth = '')
    {
        $this->config = [
            'host' => $host,
            'port' => $port,
            'auth' => $auth,
        ];
    }

    /**
     * Get redis.
     *
     * @return Redis
     * @throws RedisException
     */
    protected function getRedisInstance()
    {
        if (!isset(static::$redis)) {
            $redis = new \Redis();
            $redis->connect($this->config['host'], $this->config['port']);

            if (!$redis->auth($this->config['auth'])) {
                throw new RedisException("redis password verification failed, auth={$this->config['auth']}");
            }

            if ($redis->ping() !== '+PONG') {
                throw new RedisException("redis connection is not available, ping={$redis->ping()}");
            }

            static::$redis = $redis;
        }

        return static::$redis;
    }

    /**
     * Get task pool key.
     *
     * @return string
     */
    protected function getTaskPoolKey()
    {
        return $this->prefix . 'task_pool';
    }

    /**
     * Get task key.
     *
     * @param string $topic
     * @param mixed  $id
     *
     * @return string
     */
    public function getTaskKey(string $topic, $id)
    {
        return $topic . ':' . $id;
    }

    /**
     * Get bucket key.
     *
     * @param string $topic
     * @param bool   $flag
     *
     * @return string
     */
    protected function getBucketKey(string $topic = '', $flag = false)
    {
        $key = crc32($topic) % $this->bucketCount;
        return $this->prefix . 'task_bucket:' . ($flag !== false ? $flag : $key);
    }

    /**
     * Get queue key.
     *
     * @param int $priority
     *
     * @return string
     */
    protected function getQueueKey(int $priority)
    {
        return $this->prefix . 'task_queue:' . $priority;
    }

    /**
     * Lock.
     *
     * @param string $taskKey
     *
     * @return array|bool
     */
    protected function lock(string $taskKey)
    {
        $redis = $this->getRedisInstance();
        $lockKey = $this->prefix . 'task_lock:' . $taskKey;
        $lockValue = md5(uniqid(md5(microtime(true)), true));
        $status = $redis->set($lockKey, $lockValue, ['nx', 'px' => 3000]);
        if ($status) {
            return [$lockKey, $lockValue];
        }

        return false;
    }

    /**
     * Unlock.
     *
     * @param array $lockData
     *
     * @return bool
     */
    protected function unlock(array $lockData)
    {
        $redis = $this->getRedisInstance();
        $script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";

        return $redis->eval($script, $lockData, 1);
    }
}