<?php
namespace wangb\mq;

use yii;
use yii\base\Component;
use Closure;

/**
 * mq回调
 *
 * 基础构架部提供的包不支持匿名函数，通过该组件解决
 */
class Callback extends Component
{
    /**
     * @var Closure
     */
    protected $callback = null;

    /**
     * @inheritdoc
     */
    public function __construct(Closure $callback)
    {
        parent::__construct([]);
        $this->callback = $callback;
    }

    /**
     * 回调
     *
     * @var mixed $data
     * @param \xcmq\module\Xcmq_Abstract $client
     * @return mixed
     */
    public function callback($data, $client)
    {
        Yii::info($data, __METHOD__);
        return call_user_func_array($this->callback, func_get_args());
    }
}
