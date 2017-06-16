<?php
namespace app\phpunit\src\swoole;

use \app\swoole\Server;
use app\phpunit\base\TestCase;

class ServerTest extends TestCase
{


    /**
     * @var AdminLogComponent
     */
    public function setUp()
    {
        parent::setUp();
        $this->class = new Server();
    }

    public function testGetClient()
    {
         $this->assertSame(1, $this->class->getServer());
    }
}
