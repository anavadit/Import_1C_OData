<?php

namespace console\modules\import\components\import;

use common\models\PersonalDepartmentBranch;
use Yii;
use yii\db\Exception;
use yii\web\HttpException;

use common\models\PersonalStatement;
use common\models\PersonalTabel;
use common\models\UserPerson;
use common\models\UserDeputy;
use common\models\OdataServer;
use common\models\Organization;
use common\models\Personal;
use common\models\Payment;
use common\models\User;
use common\models\Report;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class Import extends AImport
{
    public $odata_client;

    protected $oOdataServerModel;
    protected $oOrganizationModel;

    protected $active_pers = [];
    protected $active_pers_private = [];

    /**
     * Fabric method - возвращает нужный объект импорта в зависимости версии ЗУП 1С
     * @param object (OdataServer|Organization) $oOdata
     * @return Import2_5|Import3_0|null
     */
    public static function getImport($oOdata)
    {

        if (!($oOdata instanceof OdataServer || $oOdata instanceof Organization)) {
            Yii::error('Not OdataServer object & not Organization object', 'import');
            throw new HttpException(403, "Not OdataServer object & not Organization object");
        }

        switch ($oOdata->version->version) {
            case '3_0':
                $oImport = new Import3_0();
                break;
            case '2_5':
                $oImport = new Import2_5();
                break;
            default:
                $oImport = null; // иначе PhpStorm ругается, что переменная не определена может быть
                Yii::error('Unknown import version', 'import');
                throw new HttpException(403, "Unknown import version");
                break;
        }

        if ($oOdata instanceof OdataServer) {
            $oImport->oOdataServerModel = $oOdata;
            $oImport->setVersion($oImport->oOdataServerModel->version->version);
        } elseif ($oOdata instanceof Organization) {
            $oImport->oOrganizationModel = $oOdata;
            $oImport->setVersion($oImport->oOrganizationModel->version->version);
        }

        return $oImport;
    }



    /**
     * RabbitMQ  - Закидываем в очередь сообщения о необходимости обновления организаций
     * @return mixed
     */
    public function odata_orgs_update()
    {
        $params = \Yii::$app->params['rabbitmq'];
        $exchange = 'odata';
        $queue = 'comp';

        $connection = new AMQPStreamConnection($params['host'], $params['port'], $params['user'], $params['password'], $params['vhost']);
        $channel = $connection->channel();
        $channel->queue_declare($queue, false, true, false, false);
        $channel->exchange_declare($exchange, 'direct', false, true, false);
        $channel->queue_bind($queue, $exchange);

        foreach ($this->oOdataServerModel->active_orgs as $active_org_guid => $active_org_id) {
            $message = new AMQPMessage(json_encode([
                'odata_server_id' => $this->oOdataServerModel->id,
                'odata_guid'      => $active_org_guid,
                'odata_id'        => $active_org_id,
            ]), [
                'content_type' => 'text/plain',
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT
            ]);
            $channel->basic_publish($message, $exchange);
        }
        $channel->close();
        $connection->close();

        Yii::info('"odata_orgs_update" method is ended... The queue of active organizations of '.$this->oOdataServerModel->id.' server_id was sent.', 'import');
        return true;
    }


    /**
     * Получаем список текущих активных организаций
     * @param bool|int $active = 0|1|null
     * @return bool
     */
    public function odata_get_server_orgs($active = null)
    {
        $query = Organization::find()
            ->where(['odata_server_id' => $this->oOdataServerModel->id]);

        if (is_numeric($active)) {
            $query
                ->andWhere(['active' => $active]);
        }

        $db_orgs = $query
            ->all();

        if ($db_orgs) {

            foreach ($db_orgs as $db_org) {
                $this->oOdataServerModel->active_orgs[$db_org->guid] = $db_org->id;
            }
            unset($db_orgs);

        } else {
            Yii::info('The list of active organizations is empty of server_id='.$this->oOdataServerModel->id, 'import');
            return false;
        }

        Yii::info('The list of active organizations of server_id='.$this->oOdataServerModel->id.' were received.', 'import');
        return true;
    }


    /**
     * Обновляем активных сотрудников
     * @return bool
     */
    protected function odata_get_active_personals()
    {
        $aActivePersons = Personal::find()
            ->where([
                'organization_id' => $this->oOrganizationModel->id,
                'active' => 1])
            ->all();
        foreach ($aActivePersons as $oActivePerson) {
            $this->active_pers[$oActivePerson->guid] = [
                'id' => $oActivePerson->id,
                'private_guid' => $oActivePerson->private_guid,
            ];
            $this->active_pers_private[$oActivePerson->private_guid] = [
                'id' => $oActivePerson->id,
                'guid' => $oActivePerson->guid,
            ];
        }

        Yii::info('All active persons of organization.id '.$this->oOrganizationModel->id.' were fetch into "active_pers" var', 'import');

        unset($aActivePersons);
        return false;
    }

    /**
     * Аккумулируем все выплаты, вычеты, удержания в единую сумму для удобного отображения пользователю
     * @return mixed
     */
    protected function odata_fix_payments()
    {
        //Корректировка отрицательных выплат
        $payments = Payment::find()
            ->joinWith('personal')
            ->where(['personal.organization_id' => $this->oOrganizationModel->id])
            ->andwhere(['>', 'payment.payment_total',  0])
            ->all();

        echo count($payments)." payments were found...\r\n";

        foreach ($payments as $payment) {

            if (!Payment::find()
                ->where(['id' => $payment->id])
                ->one()) {
                continue;
            }

            $other_same_date = Payment::find()
                ->where(['guid' => $payment->guid])
                ->andwhere(['personal_id' => $payment->personal_id])
                ->andwhere(['<>', 'id', $payment->id])
                ->all();

            foreach ($other_same_date as $odate) {

                $payment->payment_total +=  $odate->payment_total;

                if ($payment->save()) {
                    Yii::info('Payment '.$payment->id.' of person '.$payment->personal_id.' was saved', 'import');
                } else {
                    Yii::error('Payment '.$payment->id.' was not saved. Error', 'import');
                    throw new Exception('Payment '.$payment->id.' was not saved. Error', $payment->getErrors());
                }

                $odate->delete();
            }

        }
    }

    /**
     * Создание финансового отчёта для каждой новой организации
     * @param $orgId
     * @throws Exception
     */
    protected function create_financial_report($orgId)
    {
        $aData = [
            'organization_id' => $orgId,
            'type' => 'financial',
            'title' => 'Финансовый отчёт',
        ];
        $oFinancial = Report::findOne($aData);
        if (!$oFinancial) { // для каждой новой организации создаём финансовый отчёт по умолчанию
            $oFinancial = new Report($aData);
            if (!$oFinancial->save()) {
                var_dump($oFinancial->getErrors());
                Yii::error('Financial report for organization ID '.$orgId.' was not created...', 'import');
                throw new Exception('Financial report for organization ID '.$orgId.' was not created...', $oFinancial->getErrors());
            }
        }
        return;
    }

    /**
     * Обновление пользователей - связи с персонами (если персоны были удалены при импорте): User, UserPerson, UserDeputy
     * @throws Exception
     */
    protected function update_users()
    {
        $aOrgPersons = Personal::find()
            ->select(['guid', 'private_guid', 'id', 'organization_id'])
            ->where(['organization_id' => $this->oOrganizationModel->id])
            ->indexBy('guid')
            ->all();

        if ($aOrgPersons) {
            foreach ($aOrgPersons as $guid => $oPerson) {

                // Обновление пользователей в таблице user
                $oUser = User::findOne(['personal_guid' => $oPerson->guid]);
                if ($oUser) {
                    $oUser->personal_id = $oPerson->id;
                    if (!$oUser->auth_key) { // auth_key не может быть пустым, нужен для идентификации юзера в identity методе
                        $oUser->generateAuthKey();
                    }
                    if (!$oUser->save()) {
                        Yii::error("Personal_id = " . $oPerson->id . " was not saved in `user`", 'import');
                        throw new HttpException(403, print_r($oUser->getErrors(), true));
                    }
                }

                // Обновление сотрудников среди руководителей и подчинённых  в таблице user_person
                $aUserPersonParent = UserPerson::findAll(['personal_parent_guid' => $oPerson->guid]);
                if ($aUserPersonParent) {
                    foreach($aUserPersonParent as $oUserPersonParent) {
                        $oUserPersonParent->personal_parent_id = $oPerson->id;
                        if (!$oUserPersonParent->save()) {
                            throw new HttpException(403, print_r($oUserPersonParent->getErrors(), true));
                        }
                    }
                }
                $aUserPersonChild = UserPerson::findAll(['personal_child_guid' => $oPerson->guid]);
                if ($aUserPersonChild) {
                    foreach($aUserPersonChild as $oUserPersonChild) {
                        $oUserPersonChild->personal_child_id = $oPerson->id;
                        if (!$oUserPersonChild->save()) {
                            throw new HttpException(403, print_r($oUserPersonChild->getErrors(), true));
                        }
                    }
                }

                // Обновление сотрудников среди руководителей и заместителей в таблице user_deputy
                $aUserDeputyParent = UserDeputy::findAll(['personal_parent_guid' => $oPerson->guid]);
                if ($aUserDeputyParent) {
                    foreach($aUserDeputyParent as $oUserDeputyParent) {
                        $oUserDeputyParent->personal_parent_id = $oPerson->id;
                        if (!$oUserDeputyParent->save()) {
                            throw new HttpException(403, print_r($oUserDeputyParent->getErrors(), true));
                        }
                    }
                }
                $aUserDeputyDeputy = UserDeputy::findAll(['personal_deputy_guid' => $oPerson->guid]);
                if ($aUserDeputyDeputy) {
                    foreach($aUserDeputyDeputy as $oUserDeputyDeputy) {
                        $oUserDeputyDeputy->personal_deputy_id = $oPerson->id;
                        if (!$oUserDeputyDeputy->save()) {
                            throw new HttpException(403, print_r($oUserDeputyDeputy->getErrors(), true));
                        }
                    }
                }

            }
        }
    }

    /**
     * Обновление заявлений - связи с персонами (если персоны были удалены при импорте)
     * @throws Exception
     */
    protected function update_statements()
    {
        $aOrgPersons = Personal::find()
            ->where(['organization_id' => $this->oOrganizationModel->id])
            ->all();

        if ($aOrgPersons) {
            foreach ($aOrgPersons as $oPerson) {
                $oPersonalStatement = PersonalStatement::findOne(['personal_guid' => $oPerson->guid]);
                if (!$oPersonalStatement) { // если сотрудник, а не менеджер и не админ, то привязан к персоне
                    continue;
                }
                $oPersonalStatement->personal_id = $oPerson->id;
                if (!$oPersonalStatement->save()) {
                    Yii::error("Personal_id = " . $oPerson->id . " was not saved in `user`", 'import');
                    throw new HttpException(403, print_r($oPersonalStatement->getErrors(), true));
                }
            }
        }
    }

    /**
     * Обновление рабочих табелей - связи с персонами (если персоны были удалены при импорте)
     * @throws Exception
     */
    protected function update_tabels()
    {
        $aOrgPersons = Personal::find()
            ->where(['organization_id' => $this->oOrganizationModel->id])
            ->all();

        if ($aOrgPersons) {
            foreach ($aOrgPersons as $oPerson) {
                $oPersonalTabel = PersonalTabel::findOne(['personal_guid' => $oPerson->guid]);
                if (!$oPersonalTabel) { // если сотрудник, а не менеджер и не админ, то привязан к персоне
                    continue;
                }
                $oPersonalTabel->personal_id = $oPerson->id;
                if (!$oPersonalTabel->save()) {
                    Yii::error("Personal_id = " . $oPerson->id . " was not saved in `user`" . $oPerson->id, 'import');
                    throw new HttpException(403, print_r($oPersonalTabel->getErrors(), true));
                }
            }
        }
    }

    /**
     * Обновление сотрудников в таблице связи филиалов и отделов сотудников
     * @throws Exception
     */
    protected function update_personal_department_branch()
    {
        $aOrgPersons = Personal::find()
            ->where(['organization_id' => $this->oOrganizationModel->id])
            ->all();

        if ($aOrgPersons) {
            foreach ($aOrgPersons as $oPerson) {
                $oPersonalDepartmentBranch = PersonalDepartmentBranch::findOne(['personal_guid' => $oPerson->guid]);
                if (!$oPersonalDepartmentBranch) { // если сотрудник, а не менеджер и не админ, то привязан к персоне
                    continue;
                }
                $oPersonalDepartmentBranch->personal_id = $oPerson->id;
                if (!$oPersonalDepartmentBranch->save()) {
                    Yii::error("Personal_id = " . $oPerson->id . " was not saved in `user`" . $oPerson->id, 'import');
                    throw new Exception("Personal_id = " . $oPerson->id . " was not saved in `user`", $oPersonalDepartmentBranch->getErrors());
                }
            }
        }
    }

}