<?php
namespace wangb\mq;

use yii\helpers\ArrayHelper;
/**
 * mq配置
 */
class Config
{
    /**
     * 生成配置
     *
     * @param array $queues
     * @return array
     */
    public static function build(array $queues)
    {
        $result = [];
        foreach ($queues as $queueName => $config) {
            if (!isset($config['wait_timeout'])) {
                $timeout = $_GET['mq_wait_timeout'] ?? 60;
                $timeout = $timeout > 0 && $timeout < 60 ? $timeout : 60;
                $config['wait_timeout'] = $timeout;
            }
            $result[$queueName] = ArrayHelper::merge(
                [
                    'mq' => 'rabbitmqorg',
                    'queue' => [
                        $queueName => [
                            'queue_name' => $queueName,
                            'durable' => true,
                            'auto_delete' => false,
                            'routingkey' => $queueName
                        ],
                    ],
                    'exchange' => [
                        'exchange_name' => 'amq.direct',
                        'durable' => true,
                        'auto_delete' => false,
                        'exchange_type' => 'direct',
                    ],
                    'amqp_ex_type' => 'direct',
                    'delivery_mode' => 2,
                    'format' => 'json',
                    'wait_timeout' => null,
                ],
                $config
            );
        }
        return $result;
    }
}
