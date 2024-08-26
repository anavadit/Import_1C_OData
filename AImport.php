<?php

namespace console\modules\import\components\import;

use Yii;
use yii\base\Component;
use yii\db\Exception;
use yii\web\HttpException;

use common\models\OdataServerVersion;
use common\models\Document;
use common\models\NdflDed;
use common\models\OdataServer;
use common\models\PersonalVacation;
use common\models\Organization;
use common\models\Personal;
use common\models\Payment;
use common\models\NdflIncome;
use common\models\PersonalAdres;
use common\models\Accrual;
use common\models\Retention;

use Kily\Tools1C\OData\Client;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

abstract class AImport extends Component
{
    private static $version = null;

    public function __construct(array $config = [])
    {
        parent::__construct($config);
    }

    /**
     * @param $version (2_5|3_0)
     * @throws HttpException
     */
    protected function setVersion($version)
    {
        if (!floatval($version) or !intval($version)) {
            Yii::error('Incorrect odata version tryiing set. Now available are 2_5|3_0 only', 'import');
            throw new HttpException(403, 'Incorrect odata version tryiing set. Now available are 2_5|3_0 only');
        }
        self::$version = $version;
    }


    /**
     * @return null|string
     */
    public function getVersion()
    {
        return self::$version;
    }

}