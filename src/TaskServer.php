<?php

namespace Sevming\DelayedTask;

use Exception;
use Sevming\DelayedTask\Traits\HttpTrait;

class TaskServer extends DelayedTask
{
    use HttpTrait;

    /**
     * Log file.
     *
     * @var string
     */
    public $logFile = '';

    /**
     * The file to store master process PID.
     *
     * @var string
     */
    public $pidFile = '';

    /**
     * Debug mode.
     *
     * @var bool
     */
    public $debug = false;

    /**
     * The PID of master process.
     *
     * @var int
     */
    protected $masterPid = 0;

    /**
     * The PID Map of bucket process.
     *
     * @var array
     */
    protected $bucketPidMap = [];

    /**
     * The PID Map of queue process.
     *
     * @var array
     */
    protected $queuePidMap = [];

    /**
     * Run all.
     *
     * @throws Exception
     */
    public function runAll()
    {
        $this->checkEnv();
        $this->init();
        $this->parseCommand();
        $this->daemonize();
        $this->installSignal();
        $this->saveMasterPid();
        $this->scanBucket();
        $this->scanQueue();
        $this->resetStd();
        $this->monitorProcess();
    }

    /**
     * Check env.
     */
    protected function checkEnv()
    {
        // Only for cli
        if (php_sapi_name() != 'cli') {
            exit("only run in command line mode.\n");
        }

        // Only for linux
        if (strpos(strtolower(PHP_OS), 'win') === 0) {
            exit("Not support windows.\n");
        }

        // Need pcntl extension
        if (!extension_loaded('pcntl')) {
            exit("Please install pcntl extension.\n");
        }

        // Need posix extension
        if (!extension_loaded('posix')) {
            exit("Please install posix extension.\n");
        }

        // Need redis extension
        if (!extension_loaded('redis')) {
            exit("Please install redis extension.\n");
        }
    }

    /**
     * Init.
     */
    protected function init()
    {
        $backtrace = debug_backtrace();
        $startFile = $backtrace[count($backtrace) - 1]['file'];

        if (empty($this->pidFile)) {
            $this->pidFile = __DIR__ . '/' . str_replace('/', '_', $startFile) . '.pid';
        }

        if (empty($this->logFile)) {
            $this->logFile = __DIR__ . '/TaskServer.log';
        }

        $this->logFile = (string)$this->logFile;
        if (!is_file($this->logFile)) {
            touch($this->logFile);
            chmod($this->logFile, 0622);
        }

        $this->setProcessTitle('master');
    }

    /**
     * Parse command.
     */
    protected function parseCommand()
    {
        global $argv;

        $availableCommands = ['start', 'stop'];
        if (!isset($argv[1]) || !in_array($argv[1], $availableCommands)) {
            if (isset($argv[1])) {
                exit("Unknown command: {$argv[1]}\n");
            }
        }

        $command = isset($argv[1]) ? trim($argv[1]) : '';
        // Avoid repeated run
        $masterPid = is_file($this->pidFile) ? file_get_contents($this->pidFile) : 0;
        $masterIsAlive = $masterPid && posix_kill($masterPid, 0) && posix_getpid() != $masterPid;
        if ($masterIsAlive && $command === 'start') {
            $this->log('already running');
            exit;
        }

        switch ($command) {
            case 'stop':
                $this->log('process is stopping ...');
                // Send stop signal to master process.
                $masterPid && posix_kill($masterPid, SIGINT);
                $timeout = 5;
                $startTime = time();
                // Check master process is still alive?
                while (1) {
                    $masterIsAlive = $masterPid && posix_kill($masterPid, 0);
                    if ($masterIsAlive) {
                        if (time() - $startTime >= $timeout) {
                            $this->log('process stop fail.');
                            exit;
                        }

                        usleep(10000);
                        continue;
                    }

                    $this->log('process stop success.');
                    exit(0);
                }

                break;
        }
    }

    /**
     * Run as deamon mode.
     *
     * @throws Exception
     */
    protected function daemonize()
    {
        if ($this->debug) {
            return;
        }

        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new Exception('fork fail');
        } elseif ($pid > 0) {
            exit(0);
        }

        if (posix_setsid() === -1) {
            throw new Exception('setsid fail');
        }

        // Fork again avoid SVR4 system regain the control of terminal.
        $pid = pcntl_fork();
        if ($pid === -1) {
            throw new Exception('fork fail');
        } elseif ($pid > 0) {
            exit(0);
        }

        umask(0);
        $this->setProcessTitle('master');
    }

    /**
     * Install signal.
     */
    protected function installSignal()
    {
        // stop
        pcntl_signal(SIGINT, [$this, 'signalHandler'], false);
        // ignore
        pcntl_signal(SIGPIPE, SIG_IGN, false);
    }

    /**
     * Signal handler.
     *
     * @param $signal
     */
    protected function signalHandler($signal)
    {
        switch ($signal) {
            case SIGINT:
                $this->stopAll();
                break;
        }
    }

    /**
     * Stop all.
     */
    public function stopAll()
    {
        // For master process
        if ($this->masterPid === posix_getpid()) {
            $this->log("master process {$this->masterPid} stopping ...");

            $childPidMap = array_merge($this->bucketPidMap, $this->queuePidMap);
            foreach ($childPidMap as $pid) {
                posix_kill($pid, SIGKILL);
                $this->log("child process {$pid} stopping success.");
            }
        } // For child processes
        else {
            exit(0);
        }
    }

    /**
     * Save master pid.
     *
     * @throws Exception
     */
    protected function saveMasterPid()
    {
        $this->masterPid = posix_getpid();
        if (file_put_contents($this->pidFile, $this->masterPid) === false) {
            throw new Exception('can not save pid to ' . $this->pidFile);
        }
    }

    /**
     * Reset Std.
     */
    protected function resetStd()
    {
        if ($this->debug) {
            return;
        }

        fclose(STDIN);
        fclose(STDOUT);
        fclose(STDERR);
        $stdoutFile = '/dev/null';
        fopen($stdoutFile, 'r');
        fopen($stdoutFile, 'a');
        fopen($stdoutFile, 'a');
    }

    /**
     * Scan Bucket in real time to handle delayed tasks.
     *
     * @throws Exception
     */
    protected function scanBucket()
    {
        for ($i = 0; $i < $this->bucketCount; $i++) {
            $pid = pcntl_fork();
            if ($pid > 0) {
                $this->bucketPidMap[$pid] = $pid;
            } elseif ($pid === -1) {
                throw new Exception('bucket fork fail');
            } else {
                try {
                    // Re-establish a redis connection for each process.
                    static::$redis = null;
                    $redis = $this->getRedisInstance();
                    $taskPoolKey = $this->getTaskPoolKey();
                    $bucketKey = $this->getBucketKey('', $i);
                    $this->setProcessTitle('bucket');

                    while (1) {
                        pcntl_signal_dispatch();
                        // Get a delay task with a delay time less than or equal to the current time
                        $taskKeyArray = $redis->zRangeByScore($bucketKey, '-inf', time(), [
                            'withscores' => true,
                            'limit' => [0, $this->bucketRangeLimit],
                        ]);

                        if (!empty($taskKeyArray)) {
                            foreach ($taskKeyArray as $taskKey => $score) {
                                if ($task = $redis->hGet($taskPoolKey, $taskKey)) {
                                    // Decode task.
                                    $task = json_decode($task, true);
                                    if ($task['status'] === static::TASK_STATUS_DELAY) {
                                        // Add lock.
                                        $lockData = $this->lock($taskKey);

                                        if ($lockData) {
                                            if ($redis->zRem($bucketKey, $taskKey)) {
                                                // Add tasks to different queues based on task priority
                                                if (!$redis->rPush($this->getQueueKey($task['priority']), $taskKey)) {
                                                    $this->log("bucket rPush fail,taskKey={$taskKey}");
                                                }
                                            }

                                            // Unlock.
                                            $this->unLock($lockData);
                                        }
                                    } else {
                                        $redis->zRem($bucketKey, $taskKey);
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception $e) {
                    $this->log($e->getMessage());
                    exit(250);
                }
            }
        }
    }

    /**
     * Scan Queue in real time to handle tasks.
     *
     * @throws Exception
     */
    protected function scanQueue()
    {
        for ($i = 0; $i < $this->queueCount; $i++) {
            $pid = pcntl_fork();
            if ($pid > 0) {
                $this->queuePidMap[$pid] = $pid;
            } elseif ($pid === -1) {
                throw new Exception('queue fork fail');
            } else {
                try {
                    // Re-establish a redis connection for each process.
                    static::$redis = null;
                    $redis = $this->getRedisInstance();
                    $taskPoolKey = $this->getTaskPoolKey();
                    $this->setProcessTitle('queue');
                    $queueTaskPriority = array_reverse($this->queueTaskPriority, true);

                    while (1) {
                        pcntl_signal_dispatch();

                        foreach ($queueTaskPriority as $priority => $nums) {
                            $queueKey = $this->getQueueKey($priority);
                            // Process $nums tasks each time, avoiding the inability to handle higher priority tasks
                            while ($nums--) {
                                $taskKey = $redis->lPop($queueKey);
                                if (empty($taskKey)) {
                                    break;
                                }

                                if ($task = $redis->hGet($taskPoolKey, $taskKey)) {
                                    $task = json_decode($task, true);
                                    if ($task['status'] === static::TASK_STATUS_DELAY && $task['delayTime'] <= time()) {
                                        $newTask = $this->handleTask($task);
                                        $redis->hSet($taskPoolKey, $taskKey, json_encode($newTask));
                                        if ($newTask['status'] === static::TASK_STATUS_DELAY) {
                                            // Save the task ID, with the task delay execution time as the sort value
                                            $redis->zAdd($this->getBucketKey($newTask['topic']), $newTask['delayTime'], $taskKey);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception $e) {
                    $this->log($e->getMessage());
                    exit(250);
                }
            }
        }
    }

    /**
     * Handle task.
     *
     * @param array $task
     *
     * @return array
     */
    protected function handleTask(array $task)
    {
        if (!empty($task['url'])) {
            $result = static::request($task['url'], $task['params'], $task['method']);
        } else {
            list ($class, $action) = $task['call'];
            if (!class_exists($class) || !method_exists($class, $action)) {
                $result = 'fail';
                $this->log("call class {$class} not exists or action {$action} not exists.");
            } else {
                $result = (new $class())->$action($task);
            }
        }

        $task['lastRunTime'] = date('Y-m-d H:i:s');
        $task['rule']['count']++;
        $result = strtolower((string)$result);

        if ($result === 'success') {
            $task['status'] = static::TASK_STATUS_OK;
            if (!empty($task['callback'])) {
                list ($class, $action) = $task['callback'];
                if (!class_exists($class) || !method_exists($class, $action)) {
                    $this->log("callback class {$class} not exists or action {$action} not exists.");
                } else {
                    $result = (new $class())->$action($task);
                }
            }
        } elseif ($result === 'fail') {
            $task['status'] = static::TASK_STATUS_FAIL;
        } else {
            $index = $task['rule']['count'];
            if (!isset($task['rule']['interval'][$index]) && !$task['rule']['persistent']) {
                $task['status'] = static::TASK_STATUS_FAIL;
            } else {
                $intervalTime = $task['rule']['interval'][$index] ?? end($task['rule']['interval']);
                $delayTime = $task['delayTime'] + $intervalTime;
                $nowTime = time();
                if ($delayTime < $nowTime) {
                    $delayTime = $nowTime + $intervalTime;
                }

                $task['delayTime'] = $delayTime;
            }
        }

        return $task;
    }

    /**
     * Monitor process.
     */
    protected function monitorProcess()
    {
        while (1) {
            pcntl_signal_dispatch();
            $status = 0;
            $pid = pcntl_wait($status, WUNTRACED);
            pcntl_signal_dispatch();

            if ($pid > 0) {
                if ($status !== 0) {
                    $this->log("child process {$pid} exit with status {$status}");
                }

                unset($this->bucketPidMap[$pid], $this->queuePidMap[$pid]);
            }

            if (empty($this->bucketPidMap) && empty($this->queuePidMap)) {
                @unlink($this->pidFile);
                exit(0);
            }
        }
    }

    /**
     * Set process title.
     *
     * @param string $title
     */
    protected function setProcessTitle(string $title)
    {
        if (function_exists('cli_set_process_title')) {
            cli_set_process_title($this->prefix . $title);
        }
    }

    /**
     * Log.
     *
     * @param string $msg
     */
    protected function log(string $msg)
    {
        $msg = date('Y-m-d H:i:s') . ' [' . static::class . '] ' . $msg . PHP_EOL;
        file_put_contents((string)$this->logFile, $msg, FILE_APPEND | LOCK_EX);
    }
}