<?php
namespace wangb\mq;

use yii;
use XcmqClient;
use Closure;
use Exception;

// XcmqClient需要用到这两个常量
//mq服务端信息
if (!defined('XCMQ_CONF_FILE')) {
    define('XCMQ_CONF_FILE', Yii::getAlias('@app/config/mq.php'));
}
//mq连接配置
defined('XLOGGER_CONF_PATH') || define('XLOGGER_CONF_PATH', __DIR__ . '/config');
//加载xcmpClient类
requbase 'v1.4-beta/XcmqClient.php';

/**
 * mq组件
 */
class Mq extends yii\base\Component
{
    /**
     * @var bool 发生错误时不抛出异常？
     */
    protected $slient = false;

    /**
     * 发送mq
     *
     * @param string $queueName
     * @param mixed $data
     * @return mixed
     * @throws Exception
     */
    public function send($queueName, $data)
    {
        try {
            Yii::info([$queueName, $data], __METHOD__);
            return $this->getClient($queueName)->sendMQ($data, $queueName);
        } catch (Exception $e) {
            Yii::warning([$queueName, $data], __METHOD__);
            if ($this->slient) {
                return $e->getMessage();
            } else {
                throw $e;
            }
        }
    }

    /**
     * 获取mq队列
     *
     * @param string $queueName
     * @param callable $callback
     * @return mixed
     * @throws Exception
     */
    public function receive($queueName, callable $callback)
    {
        if ($callback instanceof Closure) {
            $callback = [new Callback($callback), 'callback'];
        }
        try {
            return $this->getClient($queueName)->receiveMQ($queueName, $callback);
        } catch (Exception $e) {
            Yii::warning($queueName, __METHOD__);
            if ($this->slient) {
                return $e->getMessage();
            } else {
                throw $e;
            }
        }
    }

    /**
     * 获取mq客户端
     *
     * @param string $queueName
     * @return \xcmq\Xcmq
     */
    public function getClient($queueName)
    {
        return XcmqClient::singleton($queueName);
    }

    /**
     * 设置调用是否抛出异常
     *
     * @param bool $slient
     * @return $this
     */
    public function slient($slient = true)
    {
        $this->slient = $slient;
        return $this;
    }
}
