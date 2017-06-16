<?php
namespace app\phpunit\src\swoole;

use app\phpunit\base\TestCase;
use \app\swoole\Client;

class ClientTest extends TestCase
{


    /**
     * @var AdminLogComponent
     */
    public function setUp()
    {
        parent::setUp();
        $this->class = new Client();
    }

    public function testGetClient()
    {
         $this->assertSame(0, $this->class->getClient());
    }
}
