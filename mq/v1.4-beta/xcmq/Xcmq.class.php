<?php 
namespace xcmq;
use xlogger\module\XLogger as XLogger;
use xcmq\module\Xcmq_ActiveMQ as Xcmq_ActiveMQ;
use xcmq\module\Xcmq_RabbitMQ as Xcmq_RabbitMQ;
use xcmq\module\Xcmq_RabbitMQORG as Xcmq_RabbitMQORG;
/**
 *  Xcmq 类
 *
 * @package module
 * @author  谢<jc3wish@126.com>
 * 
 * $Id$
 */
class Xcmq
{

    private static $_instance = array(); 
    
    private $configKey;
    
    // 配置内容
    private static $_configArr = array();
    
    // 判断文件是否加载
    private static $_configLoad=0;
    
    //配置文件中的 key 
    private $_configKey=null;
    
    //mq实例化对象
    private $_mq_conn = null;
    
    //xlogger日志模块
    private $_xlogger_module = 'xcmq';
    
    //xlogger 实例化对象
    private $_xlogger = null;
    
    //是否开启事务 
    private $_isTransaction = false;
    
    //写入数据list,用于人为开启事务
    private $_tranDataList = array();
    
    //版本
    private static $_version = 'v1.4-beta';
    
    public function getVersion(){
        return self::$_version;
    }
    
    
    public static function singleton($configKey)  
    {  
        if(!isset(self::$_instance[$configKey]))  
        {  
            $c=__CLASS__;  
            self::$_instance[$configKey] =new $c($configKey);  
        }
        return self::$_instance[$configKey];  
    }
    
    function __construct($configKey) 
    {
        $this->configKey = $configKey;
        // 配置文件加载
        if( self::$_configLoad == 0 ){
            if(defined('XCMQ_CONF_FILE')){
                $file = XCMQ_CONF_FILE;
            }else{
                $file=XCMQ_API_PATH.'/config/mq_config.php';
            }
            self::$_configArr=include_once($file);
            self::$_configLoad = 1;
        }
        $m = self::$_configArr[$configKey]['mq'];
        
        if(!isset(self::$_configArr[$configKey]['reconnect']['read']['count'])){
            self::$_configArr[$configKey]['reconnect']['read']['count'] = 0;
        }
        if(!isset(self::$_configArr[$configKey]['reconnect']['read']['sleep'])){
            self::$_configArr[$configKey]['reconnect']['read']['sleep'] = 0;
        }
        if(!isset(self::$_configArr[$configKey]['reconnect']['write']['count'])){
            self::$_configArr[$configKey]['reconnect']['write']['count'] = 0;
        }
        if(!isset(self::$_configArr[$configKey]['reconnect']['write']['sleep'])){
            self::$_configArr[$configKey]['reconnect']['write']['sleep'] = 0;
        }
        
        // 初始化xlogger        
        try{
            $this->_xlogger = XLogger::getInstance($this->_xlogger_module);
        }catch (\Exception $e){
            return false;
        }
        
        switch ($m){
            case 'rabbitmq':
                $this->_mq_conn = new Xcmq_RabbitMQ();
                break;
            case 'activemq':
                $this->_mq_conn = new Xcmq_ActiveMQ();
                break;
            case 'rabbitmqorg':
                $this->_mq_conn = new Xcmq_RabbitMQORG();
                break;
            default:
                $this->_xlogger->setM('connect_error')->setData(array('msg'=>'mq配置有问题,应是 rabbitmq,activemq,rabbitmqorg 其中一个','base'=>self::$_configArr[$this->_configKey] ));
                throw new \Exception('error:'.'mq配置有问题,应是 rabbitmq,activemq,rabbitmqorg 其中一个');
        }

        $this->_configKey = $configKey;
        $err = '';
        try{
            $this->_mq_conn->setQueInfo(self::$_configArr[$configKey]);
            $this->_mq_conn->connect();
        }catch (\Exception $e){
            $err = $e->getMessage();
            $this->_xlogger->setM('connect_error')->setData(array('msg'=>$err,'base'=>self::$_configArr[$this->_configKey] ));
            throw new \Exception('error:'.$e->getMessage().' 请检查配置mq,base等配置是否正确'.' base:'.json_encode(self::$_configArr[$this->_configKey]['base']));
        }
    }
    
    // 对象连接，如果连接出错则写入错误日志
    private function _mq_connect(){
        $err = '';
        try{
            $this->_mq_conn->reConnect();
        }catch (\Exception $e){
            $err = $e->getMessage();
        }
        if( $err ){
            $this->_xlogger->setM('connect_error')->setData(array('msg'=>$err,'base'=>self::$_configArr[$this->_configKey] ));
            return false;
        }
        return true;
    }
    
    
    public function sendMQ($data,$routingKey=''){
        if( !$data ){
            return false;
        }
        
        if( !$this->_mq_conn ){
            return false;
        }
        
        //如果开启了事务的情况下 先写入数据池中，待commit的时候再执行写入
        if( $this->_isTransaction ){
            $this->_tranDataList[] = array($data,$routingKey);
            return true;
        }
        
        $r =  $this->_mq_conn->setData($data,$routingKey);
        if(is_bool($r)){
            if( !$r ){
                $this->_xlogger->setM('sendMQ')->setData(array('status'=>$r,'data'=>$data));
                //throw new \Exception('写入失败，请验证下写入的数据是否合法');
            }
            return $r;
        }else{
            //$err = $r->getMessage();
            $this->_xlogger->setM('sendMQ')->setData((array)$r);
            
            //假如没有人工开启事务的情况下，自动重连接操作
            if(!$this->_isTransaction){
                
                $c = self::$_configArr[$this->_configKey]['reconnect']['write']['count']*1;
                $t = self::$_configArr[$this->_configKey]['reconnect']['write']['sleep'];
                $t = $t?$t:0.1;
                $t *= 1;
                
                //当设置每次写入失败重连次数大于0的情况下，默认为1，则进行重连写入
                while( $c > 0 ){
                    $c--;
                    // 假如连接操作存在异常或者返回false 则进行下一次循环
                    $r = $this->_mq_connect();
                    if( !$r ){
                        sleep($t);
                        continue;
                    }
                    //假如写入返回值非bool值 true 参数则进行直接返回
                    if( true === $this->_mq_conn->setData($data,$routingKey) ){
                        return true;
                    }else{
                        sleep($t);
                        continue;
                    }
                    
                }
            }
            //throw new \Exception($err.'-重连也失败，请查看日志'); 
            return false;
        }
    }
    
    //事务提交的时候，统一把数据写入到mq中
    private function _saveData(){
        try{
            $this->_mq_conn->begin();
            foreach( $this->_tranDataList as $k=>$v ){
                $r = $this->_mq_conn->setData($v[0],$v[1]);
                if( $r !== true ){
                    $this->_mq_conn->rollback();
                    return false;
                }
            }
            $this->_mq_conn->commit();
            
        }catch (\Exception $e){
            //throw new \Exception($e->getMessage());
            return false;
        }
        
        //提交成功,清空事务队列数据
        $this->_tranDataList[] = array();
        return true;
        
    }
    
    
    public function begin(){
        $this->_isTransaction = true;
        return true;
    }
    
    public function commit(){
        if( !$this->_isTransaction ){
            return false;
        }
        
        $c = self::$_configArr[$this->_configKey]['reconnect']['write']['count']*1;
        $t = self::$_configArr[$this->_configKey]['reconnect']['write']['sleep'];
        $t = $t?$t:0.1;
        $t *= 1;
        
        $do = false;
        while( $c > 0 ){
            $c--;
            try{
                $r = $this->_saveData();
            }catch (\Exception $e){

            }
            if( $r ){
                $do = true;
                break;
            }
            //假如连接操作存在异常或者返回false 则进行下一次循环
            $r = $this->_mq_connect();
            if( !$r ){
                sleep($t);
                continue;
            }
        }
        if( $do ){
            $this->_isTransaction = false;
            return true;
        }
        return false;
    }  
    
    public function rollback(){
        //清空事务队列数据
        $this->_tranDataList[] = array();
        
        $this->_isTransaction = false;
        
        return true;
    }
    
    public function receiveMQ($queuekey,$callback){
        if(!$callback ){
            return false;
        }
        if( !$this->_mq_conn ){
            return false;
        }
        // 如果回调是数组，第一个参数为实例化类对象，第二个参数为方法名
        if( is_array($callback) ){
            try{
                if(!method_exists($callback[0], $callback[1])){
                    $this->_xlogger->setM('receiveMQ')->setData(array('msg'=>serialize($callback). ' is not a object or "'.$callback[1]. '" not exists' ));
                    throw new \Exception('callback 参数不正确,' .$callback[1].' 方法不存在');
                    return false;
                }
            }catch (\Exception $e){
                $this->_xlogger->setM('receiveMQ')->setData(array('msg'=>$e->getMessage()));
                throw new \Exception($e->getMessage());
                return  false;
            }
        }else{
            if( !function_exists($callback) ){
                $this->_xlogger->setM('receiveMQ')->setData(array('msg'=>' function: '.$callback. ' is not a function'));
                throw new \Exception($callback .' 不是function');
                return false;
            }
        }

        $c = self::$_configArr[$this->_configKey]['reconnect']['read']['count'];
        $c = $c?$c:0;
        $c *= 1;
        //假如$c 值 ，即最大循环次数为1，则默认为2，因为要加上第一次连接操作次数
        $c = ($c==0)?1:$c;
        
        $t = self::$_configArr[$this->_configKey]['reconnect']['read']['sleep'];
        $t *= 1;

        $thec = $c;
        while( $thec > 0 ){
            $thec = $thec-1;
            // 假如连接成功，下次连接失败 可重连接次数恢复
            $r = $this->_mq_conn->getData(self::$_configArr[$this->_configKey]['queue'][$queuekey],$callback);
            $this->_xlogger->setM('receiveMQ')->setData($r);
            //sleep($t);
            //假如连接操作存在异常或者返回false,如果要重连的次数大于1，那则进行下一次循环
            if ( $thec > 0 ){
                $r = $this->_mq_connect();
                if( !$r ){
                    continue;
                }else{
                    $thec = $c;
                }
            }
        }
        return false;
    }
    
    public function close(){
        if($this->_mq_conn){
            $this->_mq_conn->close();
        }
        unset(self::$_instance[$this->configKey]);
        unset($this->_mq_conn,$this->_xlogger);
    }
    
    public function __destruct(){
        if(isset($this->_mq_conn)){
            $this->_mq_conn->close();
            unset($this->_mq_conn,$this->_xlogger);
            unset(self::$_instance[$this->configKey]);
        }

    }

}
?>