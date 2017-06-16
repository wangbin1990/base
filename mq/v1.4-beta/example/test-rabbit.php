<?php
ini_set('display_errors','On'); 
error_reporting(E_ALL);
//error_reporting(E_ERROR | E_WARNING | E_PARSE);
//error_reporting(E_ERROR);
/**
 *  API  文件基础文件 
 *
 * @package api
 * @subpackage ./
 * @author  谢<jc3wish@126.com>
 */

 /*
	配置　API　目录
 */
define('XCMQ_API_PATH',dirname(dirname(__FILE__)).'/xcmq');

/**
 * 加载必须文件
*/
class testclass{
    public function testmq($data,$obj){
        print_r($data);
        if($data['key'] == 'it is test'){
            $obj->ack();
        }
    }
    
}
define('XCMQ_CONF_FILE',dirname(__FILE__).'/config/mq_config.php');

define('XLOGGER_CONF_PATH',dirname(__FILE__).'/config/xlogger');

require_once(dirname(dirname(__FILE__)).'/XcmqClient.php');

$obj = new testclass();

//$objtest = XcmqClient::singleton('test_M_key');
$objtest = new XcmqClient('test_M_key');

echo 'version:'.$objtest->getVersion();
echo "\r\n";

$r=$objtest->sendMQ(array('key'=>'it is test'),'queue_test_name_routkey');

$objtest->close();

sleep(5);
$objtest = XcmqClient::singleton('test_M_key');
$r=$objtest->sendMQ(array('key'=>'it is test'),'queue_test_name_routkey');
$objtest->receiveMQ('queue_test_name_get',array($obj,'testmq'));

echo 'over';