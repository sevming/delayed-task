<?php

namespace Sevming\DelayedTask;

use Exception, RedisException;

class TaskClient extends DelayedTask
{
    /**
     * Add task.
     *
     * @param array $params
     *
     * @throws Exception|RedisException
     */
    public function add(array $params)
    {
        $task = $this->initTask($params);
        $redis = $this->getRedisInstance();

        $taskKey = $this->getTaskKey($task['topic'], $task['id']);
        // Save task to task pool.
        $redis->hSet($this->getTaskPoolKey(), $taskKey, json_encode($task));
        // Save the task ID, with the task delay execution time as the sort value
        $redis->zAdd($this->getBucketKey($task['topic']), $task['delayTime'], $taskKey);
    }

    /**
     * Del task.
     *
     * @param string $topic
     * @param mixed  $id
     * @param bool   $softDelete
     *
     * @return void
     * @throws Exception|RedisException
     */
    public function del(string $topic, $id, bool $softDelete = true)
    {
        $taskKey = $this->getTaskKey($topic, $id);
        $redis = $this->getRedisInstance();

        $taskPoolKey = $this->getTaskPoolKey();
        if ($task = $redis->hGet($taskPoolKey, $taskKey)) {
            if (!$softDelete) {
                $redis->hDel($taskPoolKey, $taskKey);
                return;
            }

            $task = json_decode($task, true);
            if ($task['status'] !== static::TASK_STATUS_DELETED) {
                $task['status'] = static::TASK_STATUS_DELETED;
                $redis->hSet($taskPoolKey, $taskKey, json_encode($task));
            }
        }
    }

    /**
     * Init task.
     *
     * @param array $params
     *
     * @return array
     * @throws Exception
     */
    protected function initTask(array $params)
    {
        if (empty($params['topic'])) {
            throw new Exception('topic can not be empty.');
        }

        if (empty($params['id'])) {
            throw new Exception('id can not be empty.');
        }

        if (empty($params['url']) && empty($params['call'])) {
            throw new Exception('url or call can not be empty.');
        }

        if (!empty($params['call'])) {
            list ($class, $action) = $params['call'];
            if (!class_exists($class) || !method_exists($class, $action)) {
                throw new Exception("call class {$class} not exists or action {$action} not exists.");
            }
        }

        if (!empty($params['callback'])) {
            list ($class, $action) = $params['callback'];
            if (!class_exists($class) || !method_exists($class, $action)) {
                throw new Exception("callback class {$class} not exists or action {$action} not exists.");
            }
        }

        $intervalTimeArray = [0];
        if (!empty($params['intervalTime'])) {
            $intervalTimeArray = explode(',', $params['intervalTime']);
            foreach ($intervalTimeArray as $interval) {
                if (!ctype_digit((string)$interval)) {
                    throw new Exception("intervalTime incorrect format");
                }
            }
        }

        $priority = static::QUEUE_PRIORITY_1;
        if (isset($params['priority']) && in_array($params['priority'], array_keys($this->queueTaskPriority))) {
            $priority = $params['priority'];
        }

        $time = time();
        $task = [
            'topic' => $params['topic'],
            'id' => $params['id'],
            'priority' => $priority,
            'status' => static::TASK_STATUS_DELAY,
            'url' => $params['url'] ?? '',
            'method' => $params['method'] ?? 'GET',
            'call' => $params['call'] ?? [],
            'params' => $params['params'] ?? [],
            'callback' => $params['callback'] ?? [],
            'rule' => [
                'interval' => $intervalTimeArray,
                'count' => 0,
                'persistent' => isset($params['persistent']) ? (bool)($params['persistent']) : true,
            ],
            'delayTime' => $params['delayTime'] ?? ($time + $intervalTimeArray[0]),
            'lastRunTime' => '',
            'createTime' => date('Y-m-d H:i:s', $time),
        ];

        return $task;
    }
}