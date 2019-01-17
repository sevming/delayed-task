#### 业务场景
1. 商城下单后,30分钟未支付则自动取消订单(类似订单自动退款|自动收货等都一样)
2. 实现通知失败, 0|30|60|150... 重复通知,直到对方回复

#### 参考
1. [有赞延迟队列设计](https://tech.youzan.com/queuing_delay)
2. [chenlinzhong/php-delayqueue](https://github.com/chenlinzhong/php-delayqueue)
3. [基于redis的延迟消息队列设计](https://www.cnblogs.com/peachyy/p/7398430.html)

---

#### 启动服务端
``` php
<?php
use Sevming\DelayedTask\TaskServer;

require_once 'vendor/autoload.php';

try {
    $server = new TaskServer('127.0.0.1', 6379, '123456');
    // 开启调试模式
    $server->debug = true;    
    $server->runAll();
} catch (Exception $e) {
    exit($e->getMessage());
}
```

#### 客户端
``` php
<?php
use Sevming\DelayedTask\TaskClient;

require_once 'vendor/autoload.php';

try {
    $client = new TaskClient('127.0.0.1', 6379, '123456');

    // 1. 每隔3秒调用Order:return,当处理成功后调用Order:success
    $client->add([
        'topic' => 'order:return',
        'id' => 1,
        'call' => ['Order', 'return'],
        'intervalTime' => 3,
        'callback' => ['Order', 'success'],
    ]);

    // 2. 10秒后调用Order:receipt,仅调用1次
    $client->add([
        'topic' => 'order:receipt',
        'id' => 2,
        'call' => ['Order', 'receipt'],
        'intervalTime' => 10,
        'persistent' => false,
    ]);

    // 3. 设置优先级,每隔 0|30|60秒 调用Order:timeout,调用三次后结束调用
    $client->add([
        'topic' => 'order:timeout',
        'id' => 3,
        'call' => ['Order', 'timeout'],
        'priority' => 2,
        'intervalTime' => '0,30,60',
        'persistent' => false,
    ]);

    // 4. 每隔 0|10|20秒 调用Order:refund,调用三次后,间隔时间为20秒调用一次
    $client->add([
        'topic' => 'order:refund',
        'id' => 4,
        'call' => ['Order', 'refund'],
        'intervalTime' => '0,10,20',
    ]);

    // 5. 每隔 0|15|30秒 请求一次url,请求三次后,不再请求
    $client->add([
        'topic' => 'order:notify',
        'id' => 5,
        'url' => '192.168.1.77:8080',
        'params' => [
            'content' => 'hello'
        ],
        'intervalTime' => '0,15,30',
        'persistent' => false,
    ]);

    // 6. 删除任务,根据添加任务时的topic和id
    $client->del('order:return', 1);
} catch (Exception $e) {
    exit($e->getMessage());
}
```