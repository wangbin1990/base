<?php
namespace app\phpunit\base;

use yii;
use yii\web\Application;
use yii\base\InvalidParamException;
use Closure;
use ReflectionClass;
use ReflectionProperty;

/**
 * 测试基类
 */
class TestCase extends \PHPUnit\Framework\TestCase
{
    /**
     * @inheritdoc
     */
    protected function createApp()
    {
        //new Application($GLOBALS['CONFIG']);
    }

    /**
     * @inheritdoc
    */
    public function setUp()
    {
        $this->createApp();
    }

    /**
     * @inheritdoc
     */
    public function tearDown()
    {
        $this->destroyApp();
    }

    /**
     * 消毁`Yii::$app`
     */
    protected function destroyApp()
    {
        \Yii::$app = null;
    }

    /**
     * 执行一个对象的`private`或者`protected`方法
     *
     * @param object $object
     * @param string $method
     * @param array $arguments
     * @return mixed
     */
    protected function invokeMethod($object, $method, array $arguments = [])
    {
        $reflection = new ReflectionClass($object);
        $method = $reflection->getMethod($method);
        $method->setAccessible(true);
        return $method->invokeArgs($object, $arguments);
    }

    /**
     * 获取或者设置一个对象`private`或者`protected`属性
     *
     * @param string $class
     * @param string $name
     * @param mixed $value
     * @return mixed
     */
    protected function invokeProperty($class, $name, $value = null)
    {
        if (false !== strpos($name, '.')) {
            $arr = explode('.', $name);
            if (isset($arr[2])) {
                throw new InvalidParamException(sprintf(
                    '$name最多支持两个"."，当前为"%s"',
                    $name
                ));
            }
            $name = $arr[0];
            $key = $arr[1];
        }
        $property = new ReflectionProperty($class, $name);
        $property->setAccessible(true);

        if (2 == func_num_args()) {
            $value = $property->getValue($class);
            if (isset($key)) {
                return app()->helper->arr->get($value, $key);
            } else {
                return $value;
            }
        } elseif (isset($key)) {
            $v = $property->getValue($class);
            app()->helper->arr->set($v, $key, $value);
            $property->setValue($class, $v);
        } else {
            $property->setValue($class, $value);
        }
        return null;
    }

    /**
     * 获取或者设置一个对象的`private`或者`protected`静态属性
     *
     * @param string $class
     * @param string $name
     * @param mixed $value
     * @return mixed
     */
    protected function invokeStaticProperty($class, $name, $value = null)
    {
        $argNum = func_num_args();
        $bind = Closure::bind(
            function() use ($name, $value, $argNum) {
                if (false !== strpos($name, '.')) {
                    $arr = explode('.', $name);
                    if (isset($arr[2])) {
                        throw new InvalidParamException(sprintf(
                            '$name最多支持两个"."，当前为"%s"',
                            $name
                        ));
                    }
                    $name = $arr[0];
                    $key = $arr[1];
                }
                if (2 == $argNum) {
                    /** @var array $value */
                    $value = static::${$name};
                    if (isset($key)) {
                        return app()->helper->arr->get($value, $key);
                    } else {
                        return $value;
                    }
                } elseif (isset($key)) {
                    /** @var array $v */
                    $v = static::${$name};
                    app()->helper->arr->set($v, $key, $value);
                    static::${$name} = $v;
                } else {
                    static::${$name} = $value;
                }
                return null;
            },
            null,
            $class
        );

        return $bind();
    }

    /**
     * 访问一个对象的`private`或者`protected`静态方法
     *
     * @param string $class
     * @param string $method
     * @param array $arguments
     * @return mixed
     */
    protected function invokeStaticMethod($class, $method, array $arguments)
    {
        $bind = Closure::bind(
            function() use ($method, $arguments) {
                return call_user_func_array('static::' . $method, $arguments);
            },
            null,
            $class
        );

        return $bind();
    }
}
