<?php
namespace xcmq\module;
use PhpAmqpLib\Connection\AMQPStreamConnection as AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

/**
 *  Xcmq_RabbitMQORG 类
 *
 * @package xcmq
 * @subpackage xcmq.module
 * @author  谢<jc3wish@126.com>
 *
 * $Id$
 */
class Xcmq_RabbitMQORG extends Xcmq_Abstract
{
    /**
    * @var 连接对象
    */
    protected $_conn          = NULL;

    //连接及配置参数
    protected $_config        = array();

    //消息队列载体对象池

    private $_queueArr      = array();

    // 当前消息交换机 对象
    private $_exchange      = null;

    // 当前获取的队列的消息对象
    private $_message       = null;

    // 消息持久化
    private $_delivery_mode = 2;

    //回调函数
    private $_callable = '';

    // queue routingkey 关系数组
    private $_routKeyQueue = array();

    //是否已经写入队列信息exchange_declare ，queue_declare 等操作初始化
    private $_isInitSetInfo = false;

    /**
    * 构造函数
    */
    public function __construct()
    {

    }

    //重新连接
    public function reConnect(){
       try{
           $r = $this->_conn->close();
       }catch (\Exception $e){
           return $this->connect();
       }
       return $this->connect();
    }

    public function connect(){
        $write_timeout = (isset($this->_config['write_timeout']) && is_numeric($this->_config['write_timeout']) )?$this->_config['write_timeout']:3;
        $read_timeout = (isset($this->_config['read_timeout']) && is_numeric($this->_config['read_timeout']))?$this->_config['read_timeout']:3;
        $heartbeat = (isset($this->_config['heartbeat'])  && is_numeric($this->_config['heartbeat']) )?$this->_config['heartbeat']:90;
        try{
            $this->_conn = new AMQPStreamConnection(
                $this->_config['base']['host'],
                $this->_config['base']['port'],
                $this->_config['base']['login'],
                $this->_config['base']['password'],
                $this->_config['base']['vhost'],
                'false',
                'AMQPLAIN',
                null,
                'en_US',
                $write_timeout,
                $read_timeout,
                null,
                false,
                $heartbeat
                );
            $this->_channel = $this->_conn->channel();
        }catch (\Exception $e) {
           throw new \Exception($e->getMessage());
           return false;
        }
        unset($this->_config['base']['connect_timeout']);

        if(isset($this->_config['prefetchSize']) || isset($this->_config['prefetchCount'])){
            if( !isset($this->_config['prefetchSize']) ){
                $this->_config['prefetchSize'] = 0;
            }
            if( !isset($this->_config['prefetchCount']) ){
                $this->_config['prefetchCount'] = 1;
            }
            $this->_channel->basic_qos($this->_config['prefetchSize'],$this->_config['prefetchCount'],false);//false非连接全局
        }

        return true;

    }

    /**
    * exchange_declare 交换机创建
    */
    private function _exchange_declare(){
        $exchangeName = $this->_config['exchange']['exchange_name'];
        $exchangeNameAutoDelete = $this->_config['exchange']['auto_delete']===true?true:false;
        $exchangeNameDurable = $this->_config['exchange']['durable']===true?true:false;
        $exchange_type = $this->_config['exchange']['exchange_type']?$this->_config['exchange']['exchange_type']:$this->_config['amqp_ex_type'];
        $this->_channel->exchange_declare($exchangeName, $exchange_type, false, $exchangeNameDurable, $exchangeNameAutoDelete);
    }

    /**
    * 队列写的绑定routingkey ，exchange_declare,queue_declare 等操作初始化
    * return bool
    */
    private function _initSetInfo(){
        if($this->_isInitSetInfo){
            return true;
        }

        $this->_exchange_declare();

        if( $this->_config['delivery_mode'] ){
            $this->_delivery_mode = $this->_config['delivery_mode'];
        }

        foreach( $this->_config['queue'] as $k=>$v ){
            $queueName = $v['queue_name'];
            $AMQP_DURABLE =$v['durable']===true?true:false;
            $AMQP_AUTODELETE = $v['auto_delete']===true?true:false;
            $this->_channel->queue_declare($queueName, false, $AMQP_DURABLE, false, $AMQP_AUTODELETE);
            $this->_channel->queue_bind($queueName, $this->_config['exchange']['exchange_name'],$v['routingkey']);
            $this->_routKeyQueue[$v['routingkey']][] = $queueName;
        }
        $this->_isInitSetInfo = true;
        return true;
    }

    /**
    * 队列消费，exchange_declare,queue_declare 等操作初始化
    * return bool
    */
    private function _initGetInfo($v){
        //假如写入参数都初始化过了，就不用再做任何操作了，因为写入初始化都操作过了
        if($this->_isInitSetInfo){
            return true;
        }

        $this->_exchange_declare();
        //通过queue_name判断是不是当前要进行 queue_declare 的队列，如果是的话，则进行queue_declare ，否则跳过
        $queueName = $v['queue_name'];
        $AMQP_DURABLE =$v['durable']===true?true:false;
        $AMQP_AUTODELETE = $v['auto_delete']===true?true:false;
        $this->_channel->queue_declare($queueName, false, $AMQP_DURABLE, false, $AMQP_AUTODELETE);
        return true;
    }

    /**
    * 往队列中写入数据
    * return bool
    */
    public function setData($data,$routingkey=''){
        if( !$data ){
            return false;
        }
        $this->_initSetInfo();

        if( $routingkey ){
            if(!isset($this->_routKeyQueue[$routingkey])){
                return false;
            }
            $routKeyArr[$routingkey] = '';
        }else{
            $routKeyArr = $this->_routKeyQueue;
        }

        $data = $this->encodeData($data,$this->_config['format']);

        try{

            foreach ( $routKeyArr as $k=>$v ){
                $this->_channel->basic_publish(new AMQPMessage($data,array('delivery_mode'=>$this->_delivery_mode)), $this->_config['exchange']['exchange_name'], $k);
            }
        }catch (\Exception $e){
            return $e;
        }
        return true;
    }

    /**
    * 获取到队列信息后回调方法
    */
    public function _callbackFunction($msg){
        $this->delivery_tag = $msg->delivery_info['delivery_tag'];

        $data = $this->decodeData($msg->body,$this->_config['format']);

        call_user_func($this->_callable,$data,$this);
    }

    /**
    * 消费队列中的信息
    * param $queue    队列名称
    * param $callback 回调方法
    * return array    消费过程中连接等断开异常信息
    */
    public function getData($queue,$callback){
        if(!$queue || !$callback){
            return array( 'status'=>false,'msg'=>'queue 或者 callback 为空' );
        }
        $this->_callable  = $callback;
        $queueName = $queue['queue_name'];
        try{
            $this->_initGetInfo($queue);
            $this->_channel->basic_consume($queueName, '', false, false, false, false, array($this,'_callbackFunction'));
            while(count($this->_channel->callbacks)) {
                $this->_channel->wait(
                    null,
                    false,
                    isset($this->_config['wait_timeout']) ? $this->_config['wait_timeout'] : 0
                );
            }
        }catch (\Exception $e){
            return array( 'status'=>false,'msg'=>$e->getMessage() );
        }
    }

    public function begin(){
        $this->_channel->tx_select();
        return true;
    }

    public function commit(){
        $this->_channel->tx_commit();
        return true;
    }

    public function rollback(){
        $this->_channel->tx_rollback();
        return true;
    }

    public function ack(){
        try{
             $this->_channel->basic_ack($this->delivery_tag);
        }catch (\Exception $e){
            return false;
        }
        return true;
    }

    public function close(){
        try{
            if($this->_conn){
                $this->_channel->close();
                $this->_conn->close();
            }
        }catch (\Exception $e){

        }
    }

    public function __destruct(){
        try{
            if(isset($this->_conn) && $this->_conn){
                $this->_channel->close();
                $this->_conn->close();
            }
        }catch (\Exception $e){

        }
    }
}
?>