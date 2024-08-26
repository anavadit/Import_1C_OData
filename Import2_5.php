<?php

namespace console\modules\import\components\import;

use console\modules\import\components\import\interfaces\IImport;

use Yii;
use yii\web\HttpException;
use yii\db\Exception;

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

class Import2_5 extends Import implements IImport
{

    /*-------------------- Server-side functions: раньше были в common/models/OdataServer --------------------*/

    /**
     * Обновление данных на сервере - Стартовая функция - начало импорта
     * @param $oOdataServer
     * @return bool
     */
    public function odata_update()
    {
        $this->odata_client = new Client($this->oOdataServerModel->odata_host, Yii::$app->params['odata_access_config']); // доступ в common/config/params-local.php
        if (!$this->odata_client) {
            Yii::error("OData Client object was not created on Server-side. Check access config in common/config/params-local.php", 'import');
        }

        $this->odata_orgs_list_update(false);
        $this->odata_get_server_orgs(1); // 1 - только активные (те, которые нужны для импорта), 'all' - все
        $this->odata_orgs_update();
        return true;
    }

    /**
     * Обновляем список организации с сервера
     * @param $clear
     * @return mixed
     */
    public function odata_orgs_list_update($clear)
    {
        if ($clear) {
            Organization::deleteAll(['odata_server_id' => $this->oOdataServerModel->id]);
            Yii::info("All organizations which belong to ".$this->oOdataServerModel->id.' server_id were deleted from db', 'import');
        }
        $odata_orgs = $this->odata_client->{'Catalog_Организации'}->get(null);

        if ($this->odata_client->request_ok) {

            if (isset($odata_orgs['value'])) {
                if (count($odata_orgs['value'])) {

                    foreach ($odata_orgs['value'] as $odata_org) {

                        $db_organization = Organization::find()
                            ->where(['guid' => $odata_org['Ref_Key']])
                            ->one();

                        if (!$db_organization) {
                            $db_organization = new Organization([
                                'guid' => $odata_org['Ref_Key'],
                            ]);
                        }

                        //Основные реквизиты
                        $db_organization->name = $odata_org['НаименованиеСокращенное'];
                        $db_organization->inn = $odata_org['ИНН'];
                        $db_organization->ogrn = $odata_org['ОГРН'];

                        $allContacts = $this->odata_client->{'InformationRegister_КонтактнаяИнформация'}->get(null/*, "Объект eq guid'".$db_organization->guid."'"*/);
                        if ($this->odata_client->request_ok) {
                            foreach ($allContacts['value'] as $aContact) {

                                if ($aContact['Объект'] != $db_organization->guid) {
                                    continue;
                                }

                                if ($aContact['Тип'] == 'Адрес' && !$db_organization->adress_legal) {
                                    $db_organization->adress_legal = $aContact['Представление'];
                                } elseif ($aContact['Тип'] == 'Телефон' && !$db_organization->phone) {
                                    $db_organization->phone = $aContact['Представление'];
                                }
                            }

                        } else {
                            Yii::error("Bad request client odata InformationRegister_КонтактнаяИнформация", 'import');
                            throw new HttpException(403, "Bad request client odata InformationRegister_КонтактнаяИнформация for organization.guid = ".$db_organization->guid);
                        }

                        //Налоговоые реквизиты
                        // todo: нет  тега "РегистрацияВНалоговомОргане_Key"
                        /*if ($odata_org['РегистрацияВНалоговомОргане_Key']) {
                            $org_reg_nalog = $this->odata_client->{'Catalog_РегистрацииВНалоговомОргане'}->get($odata_org['РегистрацияВНалоговомОргане_Key']);
                        }*/

                        $db_organization->kpp = $odata_org['КПП'];
                        $db_organization->oktmo = $odata_org['КодПоОКТМО'];

                        $db_organization->odata_server_id = $this->oOdataServerModel->id;
                        $db_organization->updated = date('Y-m-d H:i:s');

                        if (!$db_organization->save()) {
                            Yii::error('Organization was not saved...', 'import');
                            throw new Exception("Organization was not saved...", $db_organization->getErrors());
                        }
                        unset($db_organization);
                    }

                }
            }

        } else {
            Yii::info('Bad request odata client "Catalog_Организации" '.$this->odata_client->error_message, 'import');
        }
        unset($odata_orgs);
        Yii::info('The list of organizations on server side of '.$this->oOdataServerModel->id.' server_id was updated: '.date("Y-m-d H:I:S"), 'import');
    }


    /*-------------------- Client-side functions: раньше были в common/models/Organization --------------------*/

    /**
     * @return mixed
     */
    public function odataUpdate()
    {

        $serv = OdataServer::find()
            ->where(['id' => $this->oOrganizationModel->odata_server_id])
            ->one();

        $this->odata_client = new Client($serv->odata_host, Yii::$app->params['odata_access_config']); // доступ в common/config/params-local.php
        if (!$this->odata_client) {
            Yii::error("OData Client object was not created on Client-side. Check access config in common/config/params-local.php", 'import');
            throw new HttpException(403, "OData Client object was not created on Client-side. Check access config in common/config/params-local.php");
        }

        echo "\r\nStarted updating for organization id ".$this->oOrganizationModel->id."\r\n";

        //обновляем сотрудников
        $this->odata_sotr_update(false); // false - не уд всех персон перед обновлением

        //обновляем активных сотрудников
        $this->odata_get_active_personals();

        //обновляем адреса сотрудников - пока не требуется для вывода адреса регистрации сотрудника
        $this->odata_sotr_adress_update(false);

        //обновляем отпуска сотрудников
        $this->odata_sotr_vacancy_update(true);

        //Обновляем платежи сотрудников
        $this->odata_sotr_payments_update(true);

        //Обновляем начисления сотрудников
        $this->odata_sotr_accruals_update(true); // надо удалять! иначе дубли, нет признака однозначности, все поля могут быть обновлены

        //Обновляем удержания сотрудников
        $this->odata_sotr_retention_update(true); // бязательно true  чтобы удалялись иначе дубли, т.к. нет признака для обновления

        //Обновляем доходы для ндфл сотрудников
        $this->odata_sotr_ndfl_income_update(true);

        //Обновляем вычеты для сотрудников
        $this->odata_sotr_ndfl_ded_update(true);

        $this->odata_fix_payments();

        // Если персон удалили при импорте
        echo "Started users updating...";
        $this->update_users();
        echo "...Ended users updating";

        echo "Started updating statements,tabels,department_branch...\r\n";
        $this->update_statements();
        $this->update_tabels();
        $this->update_personal_department_branch();
        echo "...Ended updating statements,tabels,department_branch\r\n";

        echo "\r\n...Successfull ending of updating of import_v2.5 for organization id ".$this->oOrganizationModel->id."\r\n";
        return true;
    }


    /**
     * Обновляем сотрудников
     * @param $clear
     * @return bool
     */
    private function odata_sotr_update($clear)
    {
        if ($clear) {
            Personal::deleteAll(['organization_id' => $this->oOrganizationModel->id]); // Удаление всех клиентов перед загрузкой обновлений
            Yii::info('All personal which belongs to '.$this->oOrganizationModel->id.' organization_id was deleted from db', 'import');
        }

        /*Собираем идентификатор паспорта физлица
        @todo добавить иссключение ошибки поиска
        */
        $doc_pasport = $this->odata_client->{'Catalog_ДокументыУдостоверяющиеЛичность'}->get(null, "Description eq 'Паспорт гражданина Российской Федерации'");
        $pasport_id = $doc_pasport['value'][0]['Ref_Key'];

        /*
        * Собираем текущие кадровые данные сотрудников организации
        */
        $current_post_odata = $this->odata_client->{'Catalog_ДолжностиОрганизаций'}->get(null);
        if ($this->odata_client->request_ok) {

            if (isset($current_post_odata['value'])) {
                if (count($current_post_odata['value'])) {
                    foreach ($current_post_odata['value'] as $post_data) {
                        $current_post_arr[$post_data['Ref_Key']] = $post_data;
                    }
                }
            }
        } else {
            Yii::error('Bad request odata client in "Catalog_ДокументыУдостоверяющиеЛичность"', 'import');
            throw new HttpException(403, 'Bad request odata client in "Catalog_ДокументыУдостоверяющиеЛичность"');
        }

        /*
         * Собираем подразделения организации
         */
        $departments_arr = [];
        $dep_odata = $this->odata_client->{'Catalog_ПодразделенияОрганизаций'}->get(null, "Ref_Key eq guid'".$this->oOrganizationModel->guid."'");

        if ($this->odata_client->request_ok) {
            if (isset($dep_odata['value'])) {
                if (count($dep_odata['value'])) {
                    foreach ($dep_odata['value'] as $dep) {
                        $departments_arr[$dep['Ref_Key']] = $dep['Description'];
                    }
                }
            }
        } else {
            Yii::error('Bad request odata client in "Catalog_ПодразделенияОрганизаций"', 'import');
            throw new HttpException(403, 'Bad request odata client in "Catalog_ПодразделенияОрганизаций"');
        }

        /*
         * Собираем  должности организации
        */
        $current_cadr_odata = $this->odata_client->{'InformationRegister_РаботникиОрганизаций'}->get(null/*, "Организация_Key eq guid'".$this->oOrganizationModel->guid."'"*/);
        if ($this->odata_client->request_ok) {
            if (isset($current_cadr_odata['value'])) {
                if (count($current_cadr_odata['value'])) {
                    foreach ($current_cadr_odata['value'] as $pers_cadr_data) {
                        $current_cadr_arr[$pers_cadr_data['RecordSet'][0]['Сотрудник_Key']] = $pers_cadr_data['RecordSet'][0];
                    }
                }
            }
        } else {
            Yii::error('Bad request odata client in "InformationRegister_РаботникиОрганизаций"', 'import');
            throw new HttpException(403, 'Bad request odata client in "InformationRegister_РаботникиОрганизаций"');
        }

        //Получаем оклад сотрудников
        $sotr_salary = $this->odata_client->{'Document_ПриемНаРаботуВОрганизацию'}->get(null, "Организация_Key eq guid'".$this->oOrganizationModel->guid."'");
        if ($this->odata_client->request_ok) {
            if (isset($sotr_salary['value'])) {
                if (count($sotr_salary['value'])) {
                    foreach ($sotr_salary['value'] as $pers_salary) {
                        $sotr_salary_arr[$pers_cadr_data['RecordSet'][0]['Сотрудник_Key']] = $pers_salary;
                    }
                }
            }
        } else {
            Yii::error('Bad request odata client in "Document_ПриемНаРаботуВОрганизацию"', 'import');
            throw new HttpException(403, 'Bad request odata client in "Document_ПриемНаРаботуВОрганизацию"');
        }

        //Получаем кадровые переводы по организации
        $sotr_transfer = $this->odata_client->{'Document_КадровоеПеремещениеОрганизаций'}->get(null, "Организация_Key eq guid'".$this->oOrganizationModel->guid."'");
        if ($this->odata_client->request_ok) {

            if (isset($sotr_transfer['value'])) {
                if (count($sotr_transfer['value'])) {

                    foreach ($sotr_transfer['value'] as $pers_transfer) {
                        $sotr_transfer_arr[$pers_cadr_data['RecordSet'][0]['Сотрудник_Key']] = $pers_transfer;
                    }
                }
            }
        } else {
            Yii::info('No "Document_КадровыйПеревод" for '.$this->oOrganizationModel->guid.' organization', 'import');
        }

        /*
         * Получаем список сотрудников - Основной Цикл
         */
        $sotr = $this->odata_client->{'Catalog_СотрудникиОрганизаций'}->get(null, "Организация_Key eq guid'".$this->oOrganizationModel->guid."' and DeletionMark eq false"); // todo: ВАрхиве eq false добавить - в 2.5? - нет тега

        if (isset($sotr['value'])) {
            if (count($sotr['value'])) {

                foreach ($sotr['value'] as $skey => $svalue) {

                    $db_person = Personal::find()
                        ->where(['guid' => $svalue['Ref_Key']])
                        ->one();

                    if (!$db_person) {
                        $db_person = new Personal([
                            'guid' => $svalue['Ref_Key'],
                        ]);
                    }

                    // если нет в импорте|персона уволена, то удаляли персону из базы раньше
                    if (!isset($current_cadr_arr[$db_person->guid]) ||
                        ($svalue['ДатаУвольнения'] != '0001-01-01T00:00:00' ||
                            $svalue['ДатаПриемаНаРаботу'] == '0001-01-01T00:00:00'
                        )
                    ) {
                        var_dump('continued '.$db_person->guid."\r\n");
                        Yii::info('continued '.$db_person->guid, 'import');
                        continue;
                    }

                    $db_person->private_guid = $svalue['Физлицо_Key'];


                    if( $svalue['Ref_Key'] == $db_person->guid) {
                        if (isset($svalue['ТекущееПодразделениеОрганизации_Key'])) {
                            $db_person->department = $svalue['ТекущаяДолжностьОрганизации_Key'];
                        }

                        if (isset($current_post_arr[$current_cadr_arr[$db_person->guid]['Должность_Key']])) {
                            $db_person->post = $current_post_arr[$current_cadr_arr[$db_person->guid]['Должность_Key']]["Description"];
                            $db_person->aproove_date =  $svalue['ДатаПриемаНаРаботу']; // $current_cadr_arr[$db_person->guid]['ДатаПриемаНаРаботу'];
                        }
                    }

                    $sotr_data = $this->odata_client->{'Catalog_ФизическиеЛица'}->get($db_person->private_guid);

                    if ($this->odata_client->request_ok) {

                        if (isset($sotr_data['Description'])) { // в 2.5 версии ФИО хранится строкой
                            $personFullName = explode(' ', $sotr_data['Description']);
                            if (!empty($personFullName)) {
                                $db_person->surname = $personFullName[0]; // Фамилия
                                $db_person->first_name = $personFullName[1]; // Имя
                                $db_person->last_name = $personFullName[2]; // Отчество
                            }
                        }
                        $db_person->birth_date = $sotr_data['ДатаРождения'];
                        $db_person->organization_id = $this->oOrganizationModel->id;

                        if (isset($sotr_salary_arr[$db_person->guid])) {
                            $db_person->salary = $svalue['ТарифнаяСтавка'];
                        }

                        if (isset($sotr_transfer_arr[$db_person->guid])) {
                            $db_person->salary = $svalue['ТарифнаяСтавка'];
                        }

                        $db_person->updated = date('Y-m-d H:i:s');

                        // Сохранение персоны в базе:
                        if ($db_person->save()) {

                            $this->active_pers[$db_person->guid] = [
                                'id' => $db_person->id,
                                'private_guid' => $db_person->private_guid
                            ];

                            $this->active_pers_private[$db_person->private_guid] = [
                                'id' => $db_person->id,
                                $db_person->guid,
                            ];

                            Yii::info('Информация о person.id '.$db_person->id.' обновлена '.date('Y-m-d H:i:s'), 'import');
                        } else {
                            Yii::error('Персона с person.id '.$db_person->id.' не сохранена.', 'import');
                            throw new Exception('Персона с person.id '.$db_person->id.' не сохранена.', $db_person->getErrors());
                        }

                        //Заполнение документов сотрудника
                        if (isset($sotr_data['ИНН'])) {
                            $inn = Document::find()->where([
                                'document_type' => 'Инн',
                                'guid' => $db_person->guid,
                            ])->one();

                            if (!$inn) {
                                $inn = new Document([
                                    'guid' => $db_person->guid,
                                    'document_type' => 'Инн',
                                    'personal_id' => $db_person->id,
                                    'dval1' => $sotr_data['ИНН']

                                ]);
                                Yii::info('У персоны person.id '.$db_person->id.' добавлен новый ИНН.', 'import');
                            }

                            if ($inn->save()){
                                Yii::info('Инн с document.id '.$inn->id.' был сохранён для персоны с person.id '.$db_person->id, 'import');
                            } else {
                                Yii::error('Инн персоны '.$db_person->id.' не сохранён', 'import');
                                throw new Exception('Инн персоны '.$db_person->id.' не сохранён', $inn->getErrors());
                            }
                        }

                        if (isset($sotr_data['СтраховойНомерПФР'])) {
                            $snpfr = Document::find()->where([
                                'document_type' => 'Снилс',
                                'guid' => $db_person->guid,
                            ])->one();

                            if (!$snpfr) {
                                $snpfr = new Document([
                                    'guid' => $db_person->guid,
                                    'document_type' => 'Снилс',
                                    'personal_id' => $db_person->id,
                                    'dval1' => $sotr_data['СтраховойНомерПФР']
                                ]);
                                Yii::info('У персоны person.id '.$db_person->id.' добавлен новый СНИЛС.', 'import');
                            }

                            if ($snpfr->save()){
                                Yii::info('СНИЛС с document.id '.$snpfr->id.' был сохранён для персоны с person.id '.$db_person->id, 'import');
                            } else {
                                Yii::error('СНИЛС персоны '.$db_person->id.' не сохранён', 'import');
                                throw new HttpException(403, 'СНИЛС персоны '.$db_person->id.' не сохранён'. print_r($snpfr->getErrors(), true) );
                            }
                        }

                        $documents = $this->odata_client->{'InformationRegister_ПаспортныеДанныеФизЛиц'}->get(null, "ФизЛицо_Key eq guid'".$svalue['Физлицо_Key']."' & ДокументВид_Key eq guid'".$pasport_id."'");

                        if ($this->odata_client->request_ok) {

                            Yii::info('Начало обработки паспорта персоны с person.id '.$db_person->id.' паспорта '.$pasport_id, 'import');

                            $pass = Document::find()
                                ->where([
                                    'document_type' => 'Паспорт',
                                    'guid' => $db_person->guid,
                                ])
                                ->one();

                            $i = 1;
                            $iFresh = 0; // номер самого последнего выданного паспорта
                            while (isset($documents['value'][$i])) { // если есть хоть один паспорт, то ищем самый свежий документ по дате выдачи
                                $iFresh = strtotime($documents['value'][$i]['ДокументДатаВыдачи']) > strtotime($documents['value'][$i-1]['ДокументДатаВыдачи']) ? $i : ($i - 1);
                                $i++;
                            }

                            if (!$pass) {  // если нет паспорта, то создаём его для персоны
                                $pass = new Document([
                                    'guid' => $db_person->guid,
                                    'document_type' => 'Паспорт',
                                    'personal_id' => $db_person->id,
                                    'dval1' => isset($documents['value'][$iFresh]['ДокументСерия']) ? $documents['value'][$iFresh]['ДокументСерия'] : null,
                                    'dval2' => isset($documents['value'][$iFresh]['ДокументНомер']) ? $documents['value'][$iFresh]['ДокументНомер'] : null,
                                    'dval3' => isset($documents['value'][$iFresh]['ДокументДатаВыдачи']) ? $documents['value'][$iFresh]['ДокументДатаВыдачи'] : null,
                                    'dval4' => isset($documents['value'][$iFresh]['ДокументКемВыдан']) ? $documents['value'][$iFresh]['ДокументКемВыдан'] : null,
                                    'dval5' => isset($documents['value'][$iFresh]['ДокументКодПодразделения']) ? $documents['value'][$iFresh]['ДокументКодПодразделения'] : null,
                                ]);
                                Yii::info('Персоне с person.id '.$db_person->id.' добавлен новый паспорт.', 'import');
                            }

                            if ($pass->save()) { // Сохранение паспортных данных!
                                Yii::info('Персоне с person.id '.$db_person->id.' сохранены изменения в паспорте '.$pass->id.'. '.date('Y-m-d H:i:s'), 'import');
                            } else {
                                Yii::error('Passport of person_id '.$db_person->id.' was not saved.', 'import');
                                throw new HttpException(403, 'Passport of person_id '.$db_person->id.' was not saved.'. print_r($pass->getErrors(), true) );
                            }

                            unset($pass);
                        } else {
                            Yii::error('Bad request odata client in "InformationRegister_ДокументыФизическихЛиц" passport updating', 'import');
                            throw new HttpException(403, 'Bad request odata client in "InformationRegister_ДокументыФизическихЛиц" passport updating');
                        }

                        unset($documents);
                        unset($db_person);

                    } else {
                        Yii::error('Bad request odata client "Catalog_ФизическиеЛица" '.$this->odata_client->error_message, 'import');
                        throw new HttpException(403, 'Bad request odata client "Catalog_ФизическиеЛица" '.$this->odata_client->error_message);
                    }


                }
            }
        }

        unset($sotr);
        unset($sotr_transfer_arr);
        unset($sotr_salary_arr);
        unset($dep_odata);
        unset($current_cadr_odata);

        unset($current_post_odata);
        unset($doc_pasport);
        unset($pasport_id);
        return true;
    }

    /**
     * Обновляем адреса сотрудников
     * @param $clear
     */
    private function odata_sotr_adress_update($clear)
    {

        if ($clear) {
            $oldPersons = Personal::find()
                ->joinWith('adress')
                ->where(['personal.organization_id' => $this->oOrganizationModel->id])
                ->asArray()
                ->all();
            PersonalAdres::deleteAll(['in', 'personal_id', array_values($oldPersons)]);
            Yii::info('All address data was deletred for organization.id '.$this->oOrganizationModel->id, 'import');
            unset($oldPersons);
        }

        if (empty($this->active_pers_private)) {
            Yii::info('No active persons for updating in "odata_sotr_adress_update".', 'import');
            return;
        }

        $allContacts = $this->odata_client->{'InformationRegister_КонтактнаяИнформация'}->get(null);
        if ($this->odata_client->request_ok) {

            foreach ($allContacts['value'] as $aContact) {

                if (!isset($this->active_pers_private[$aContact['Объект']])) { // "Объект" = personal.private_guid
                    continue;
                }

                $adr = PersonalAdres::findOne([
                    'personal_id' => $this->active_pers_private[$aContact['Объект']]['id']
                ]);
                if (!$adr) {
                    $adr = new PersonalAdres([
                        'personal_id' => $this->active_pers_private[$aContact['Объект']]['id'], // здесь "Объект" = personal.private_guid
                    ]);
                }

                switch ($aContact['Тип']) {
                    case 'Адрес':
                        $adr->post_index = $aContact['Поле1'];
                        $adr->city = $aContact['Поле4'];
                        $adr->area = $aContact['Поле3']; // район
                        $adr->locality = $aContact['Поле5']; // село или деревня, обычно там, где нет города, но есть район
                        $adr->street = $aContact['Поле6'];
                        $adr->house_number = $aContact['Поле7'];
                        $adr->house_block = $aContact['Поле8'];
                        if (intval($aContact['Поле9'])) {
                            $adr->house_flat = $aContact['Поле9'];
                        }
                    break;
                    case 'Телефон':
                        $adr->phone = $aContact['Представление'];
                    break;
                }

                if (!$adr->save()) {
                    Yii::error('Save address/phone error', 'import');
                    throw new Exception('Save address error', $adr->getErrors());
                }
                unset($adr);

            }

        } else {
            Yii::error('Bad request odata client "Catalog_ФизическиеЛица" '.$this->odata_client->error_message, 'import');
            throw new HttpException(403, 'Bad request odata client "Catalog_ФизическиеЛица" '.$this->odata_client->error_message);
        }

        unset($allContacts);
    }


    /**
     * Обновляем отпуска сотрудников
     * @param $clear
     * @return mixed
     */
    private function odata_sotr_vacancy_update($clear)
    {
        $aVacationTypesRequest = $this->odata_client->{'Catalog_ВидыЕжегодныхОтпусков'}->get(null);
        $aVacationTypes = [];
        // Типы отпусков
        if ($this->odata_client->request_ok) {
            foreach ($aVacationTypesRequest['value'] as $aVacationType) {
                $aVacationTypes[$aVacationType['Ref_Key']] = $aVacationType['Description']; // Вид отпуска - оплачиваемый/отгул...
            }
        }

        foreach ($this->active_pers as $pers_guid => $pers) {

            if ($clear) {
                PersonalVacation::deleteAll(['personal_id' => $pers['id']]);
                Yii::info("All vacations deleted for person.id ".$pers['id'], 'import');
            }

            $vacations = $this->odata_client->{'AccumulationRegister_ФактическиеОтпускаОрганизаций_RecordType'}->get(null); //  "Сотрудник_Key eq guid'".$pers_guid."' and Компенсация eq false"

            if ($this->odata_client->request_ok ) {

                if (isset($vacations['value'])) {
                    if (count($vacations['value'])) {
                        foreach ($vacations['value'] as $o_vacation) {

                            if ($o_vacation['Сотрудник_Key'] != $pers_guid || $o_vacation['Компенсация']) {
                                continue;
                            }

                            if (isset($o_vacation['Period'])) { // Period = ДатаНачала в 2.5 Вова сказал
                                $db_vacation = new PersonalVacation([
                                    'date_start' => $o_vacation['Period'],
                                    'date_end' => $o_vacation['ДатаОкончания'],
                                    'vacation_count' => intval($o_vacation['Количество']),
                                    'personal_id' => $pers['id'],
                                    'description' => (isset($aVacationTypes[$o_vacation['ВидЕжегодногоОтпуска_Key']])) ? $aVacationTypes[$o_vacation['ВидЕжегодногоОтпуска_Key']] : null,
                                ]);

                                if (!$db_vacation->save()) {
                                    Yii::error('Save vacation error', 'import');
                                    throw new Exception('Save vacation error', $db_vacation->getErrors());
                                }
                                unset($db_vacation);

                            }
                        }
                    }
                } else {
                    Yii::info('Empty vacations in "AccumulationRegister_ФактическиеОтпускаОрганизаций_RecordType" ...', 'import');
                }
            } else {
                Yii::error('Bad request in "odata_sotr_vacancy_update" of odata Client "AccumulationRegister_ФактическиеОтпускаОрганизаций"', 'import');
                throw new HttpException(403, 'Bad request in "odata_sotr_vacancy_update" of odata Client "AccumulationRegister_ФактическиеОтпуска_RecordType"');
            }
        }
        unset($vacations);
        return true;
    }


    /**
     * Обновляем платежи сотрудников
     * @param $clear
     */
    private function odata_sotr_payments_update($clear)
    {
        if ($clear) {
            $old_p = Payment::find()
                ->joinWith('personal')
                ->where(['personal.organization_id' => $this->oOrganizationModel->id])
                ->asArray()
                ->all();
            Payment::deleteAll(['in', 'id', array_values($old_p)]);
            Yii::info('All payments were deleted for organization_id '.$this->oOrganizationModel->id, 'import');
            unset($old_p);
        }

        // todo: в версии 2.5 - эти ведомости не нужны. Правда? -> Артём
//            'ВедомостьНаВыплатуЗарплатыПеречислением',
//            'ВедомостьНаВыплатуЗарплатыРаздатчиком',
//            'ВедомостьНаВыплатуЗарплатыВБанк',
//            'ВедомостьНаВыплатуЗарплатыВКассу',

        //Собираем способы выплаты(аванс, зарплата)
        $payments = $this->odata_client->{'Document_ЗарплатаКВыплатеОрганизаций'}->get(null, "Организация_Key eq guid'".$this->oOrganizationModel->guid."' and DeletionMark eq false and  Posted eq true");

        if ($this->odata_client->request_ok) {

            if (count($payments['value'])) {
                foreach ($payments['value'] as $o_payment) {

                    if (isset($o_payment['Зарплата'])) {
                        foreach ($o_payment["Зарплата"] as $o_payment_entry) {

                            if (isset($this->active_pers_private[$o_payment_entry['Физлицо_Key']])) { // здесь Физлицо_Key = personal.private_guid != personal.guid !!!
                                $db_payment = new Payment();
                                $db_payment->date = date("Y-m-d H:i:s", strtotime($o_payment['Date']));
                                $db_payment->payment_period = date("Y-m-d H:i:s", strtotime($o_payment['ПериодРегистрации'])); // 'Date' в 3.0
                                $db_payment->payment_type = $o_payment['ХарактерВыплаты']; // $payment_types_arr[$o_payment['СпособВыплаты_Key']]; в 3.0
                                $db_payment->payment_total = strval($o_payment_entry['Сумма']); // 'КВыплате' в 3.0
                                $db_payment->personal_id = isset($this->active_pers_private[$o_payment_entry['Физлицо_Key']]) ? $this->active_pers_private[$o_payment_entry['Физлицо_Key']]['id'] : null; // Внимание! НЕ ошибка: присваиваем personal.guid, а не personal.private_guid
                                $db_payment->guid = $o_payment_entry['Ref_Key'];

                                if (!$db_payment->save()) {
                                    Yii::error('Payment save error', 'import');
                                    throw new Exception('Payment save error', $db_payment->getErrors());
                                }
                                unset($db_payment);
                            }
                        }
                    }

                }
            }
        } else {
            Yii::error('Bad request in "ВедомостьНаВыплатуЗарплаты..." in odata client', 'import');
            throw new HttpException(403, 'Bad request in "ВедомостьНаВыплатуЗарплаты..." in odata client');
        }
        unset($payments);
    }


    /**
     * Обновляем начисления сотрудников
     * @param bool $clear - удаляем начисления true, это надо
     */
    private function odata_sotr_accruals_update($clear)
    {

        if ($clear) {
            $old_a = Accrual::find()
                ->joinWith('personal')
                ->where(['personal.organization_id' => $this->oOrganizationModel->id])
                ->asArray()
                ->all();
            Accrual::deleteAll(['in', 'id', array_values($old_a)]);
            Yii::info('All accruals were deleted in "odata_sotr_accruals_update" for organization_id '.$this->oOrganizationModel->id, 'import');
        }

        //Собираем типы начислений
        $aAccrualResources = [
            'ОсновныеНачисленияОрганизаций',
            'ДополнительныеНачисленияОрганизаций',
        ]; // Массив с названиями ресурсов, откуда надо достать описание (название) начислений как основные так и дополнительные - так в версии 2.5

        $accrual_types_arr = [];
        foreach ($aAccrualResources as $sResourceName) {

            $accrual_types = $this->odata_client->{'ChartOfCalculationTypes_' . $sResourceName}->get();

            if ($this->odata_client->request_ok) {
                if (count($accrual_types['value'])) {

                    foreach ($accrual_types['value'] as $atype) {
                        foreach ($atype['BaseCalculationTypes'] as $aAccrualType) {
                            $accrual_types_arr[$aAccrualType['CalculationType']] = $atype['Description'];
                        }
                    }

                }
            } else {
                Yii::error('Bad request in "odata_sotr_accruals_update" for "ChartOfCalculationTypes_ОсновныеНачисленияОрганизаций" to odata client', 'import');
                throw new HttpException(403, 'Bad request in "odata_sotr_accruals_update" for "ChartOfCalculationTypes_ОсновныеНачисленияОрганизаций" to odata client');
            }
        }

        $aAccrualDataResources = [
            'ОсновныеНачисленияРаботниковОрганизаций',
            'ДополнительныеНачисленияРаботниковОрганизаций',
        ]; // МАссив с ресурсами начислений сотрудников

        $rows = [];
        foreach ($aAccrualDataResources as $aAccrualDataResource) {

            // $filter = "Организация_Key eq guid'" . $this->oOrganizationModel->guid . "' and Active eq true";
            $o_accruals = $this->odata_client->{'CalculationRegister_'.$aAccrualDataResource}->get(null/*, $filter*/);

            if ($this->odata_client->request_ok) {
                if (count($o_accruals['value'])) {

                    foreach ($o_accruals['value'] as $o_accrual) {
                        foreach ($o_accrual['RecordSet'] as $aRecord) {

                            if (!isset($this->active_pers[$aRecord['Сотрудник_Key']]) ||
                                !$aRecord['Результат'] ||
                                !isset($accrual_types_arr[$aRecord['CalculationType_Key']]) ||
                                !isset($aRecord['ОтработаноДней'])
                            ) {
                                continue;
                            }

                            switch($accrual_types_arr[$aRecord['CalculationType_Key']]) {
                                case 'Оплата по окладу':
                                    $iSort = 10;
                                    break;
                                case 'Месячная премия':
                                    $iSort = 20;
                                    break;
                                case 'Отпуск основной':
                                    $iSort = 30;
                                    break;
                                case 'Командировка':
                                    $iSort = 40;
                                    break;
                                default:
                                    $iSort = 1000;
                                    break;
                            }

                            $rows[] = [
                                $aRecord['ОтработаноДней'],
                                $aRecord['ОтработаноЧасов'],
                                $aRecord['Результат'],
                                $this->active_pers[$aRecord['Сотрудник_Key']]['id'],
                                $accrual_types_arr[$aRecord['CalculationType_Key']],
                                date("Y-m-d", strtotime($aRecord['ActionPeriod'])), // RegistrationPeriod
                                $iSort,
                            ];
                        }
                    }
                }
            } else {
                Yii::error('Bad request in "odata_sotr_accruals_update" for "CalculationRegister_Начисления_RecordType" to odata client', 'import');
                throw new HttpException(403, 'Bad request in "odata_sotr_accruals_update" for "CalculationRegister_Начисления_RecordType" to odata client');
            }

        }
        unset($o_accruals);

        if (isset($rows) and count($rows)) {
            Yii::$app->db
                ->createCommand()
                ->batchInsert('accrual', [
                    'work_days',
                    'work_hours',
                    'accrual_total',
                    'personal_id',
                    'accrual_desc',
                    'accrual_period',
                    'sort',
                ], $rows)
                ->execute();
        }
        unset($rows);
    }


    /**
     * Обновляем удержания сотрудников
     * @param $clear
     * @return mixed
     */
    private function odata_sotr_retention_update($clear)
    {
        if ($clear) {
            $old_a = Retention::find()
                ->joinWith('personal')
                ->where(['personal.organization_id' => $this->oOrganizationModel->id])
                ->asArray()
                ->all();
            Retention::deleteAll(['in', 'id', array_values($old_a)]);
            Yii::info('All retentions were deleted in "odata_sotr_retention_update" for organization_id '.$this->oOrganizationModel->id, 'import');
            unset($old_a);
        }

        //Собираем типы начислений
        $retention_types_arr = [];
        $retention_types = $this->odata_client->{'ChartOfCalculationTypes_УдержанияОрганизаций'}->get();
        if ($this->odata_client->request_ok) {

            if (count($retention_types['value'])) {
                foreach ($retention_types['value'] as $rtype) {
                    $retention_types_arr[$rtype['Ref_Key']] = $rtype['Description'];
                }
            }
        } else {
            Yii::error('Bad request in "odata_sotr_retention_update" for "ChartOfCalculationTypes_УдержанияОрганизаций"', 'import');
            throw new HttpException(403, 'Bad request in "odata_sotr_retention_update" for "ChartOfCalculationTypes_УдержанияОрганизаций"');
        }


//        $filter = "Организация_Key eq guid'".$this->oOrganizationModel->guid ."' and ГруппаНачисленияУдержанияВыплаты eq 'Удержано'";
        $o_retentions = $this->odata_client->{'CalculationRegister_УдержанияРаботниковОрганизаций'}->get(null/*, $filter*/);
        $rows = [];

        if ($this->odata_client->request_ok) {
            if (count($o_retentions['value'])) {
                $rows = [];

                foreach ($o_retentions['value'] as $o_retention) {
                    foreach ($o_retention['RecordSet'] as $aRecord) {

                        if (!isset($this->active_pers_private[$aRecord['ФизЛицо_Key']])) { // здесь в удержаниях тоже берём personal.private_guid
                            continue;
                        }

                        $rows[] = [
                            $aRecord['Результат'],
                            $this->active_pers_private[$aRecord['ФизЛицо_Key']]['id'],
                            date("Y-m-d", strtotime($aRecord['BegOfBasePeriod'])), // Period
                            (strpos($aRecord['CalculationType_Key'], '-')) ? $retention_types_arr[$aRecord['CalculationType_Key']] : $aRecord['CalculationType_Key'],
                        ];

                    }
                }

            }
        } else {
            Yii::error('Bad request in "odata_sotr_retention_update" for "CalculationRegister_УдержанияРаботниковОрганизаций"', 'import');
            throw new HttpException(403, 'Bad request in "odata_sotr_retention_update" for "CalculationRegister_УдержанияРаботниковОрганизаций"');
        }

        unset($o_retentions);
        if (isset($rows) and count($rows)) {
            Yii::$app->db
                ->createCommand()
                ->batchInsert('retention', [
                    'retention_total',
                    'personal_id',
                    'retentional_period',
                    'retrntional_desc',
                ], $rows)
                ->execute();
        }
        unset($retention_types_arr);
    }


    /**
     * Обновляем доходы для ндфл сотрудников
     * @param $clear
     */
    private function odata_sotr_ndfl_income_update($clear)
    {
        if ($clear) {
            $old_a = NdflIncome::find()
                ->joinWith('personal')
                ->where(['personal.organization_id' => $this->oOrganizationModel->id])
                ->asArray()
                ->all();
            NdflIncome::deleteAll(['in','id', array_values( $old_a)]);
            Yii::info('Ndflincome was deleted for organization_id '.$this->oOrganizationModel->id, 'import');
            unset($old_a);
        }
        NdflIncome::deleteAll(['personal_id' => null]);

        $income_codes = [];
        $ded_codes = [];

        $income_types = $this->odata_client->{'Catalog_ДоходыНДФЛ'}->get(null, "DeletionMark eq false");

        if ($this->odata_client->request_ok) {
            if (count($income_types['value'])) {
                foreach ($income_types['value'] as $o_code) {
                    $income_codes[$o_code['Ref_Key']] = $o_code['Code'];
                }
            }
        } else {
            Yii::error('Bad request in odata client in "Catalog_ДоходыНДФЛ"', 'import');
            throw new HttpException(403, 'Bad request in odata client in "Catalog_ДоходыНДФЛ"');
        }

        $ded_types = $this->odata_client->{'Catalog_ВычетыНДФЛ'}->get(null, "DeletionMark eq false");

        if ($this->odata_client->request_ok) {
            if (count($ded_types['value'])) {
                foreach ($ded_types['value'] as $o_code) {
                    $ded_codes[$o_code['Ref_Key']] = $o_code['КодДляОтчетности2016'];
                }
            }
        } else {
            Yii::error('Bad request in odata client in "Catalog_ВычетыНДФЛ"', 'import');
            throw new HttpException(403, 'Bad request in odata client in "Catalog_ВычетыНДФЛ"');
        }

//      $filter = "ФизическоеЛицо_Key eq guid'".$pers['private_guid']."' and Active eq true";
        $ndfl_income = $this->odata_client->{'AccumulationRegister_НДФЛСведенияОДоходах'}->get(null/*, $filter*/);

        if ($this->odata_client->request_ok) {
            if (count($ndfl_income['value'])) {

                $rows = [];

                foreach ($ndfl_income['value'] as $o_income) {
                    foreach ($o_income['RecordSet'] as $aRecord) {

                        if (!isset($this->active_pers_private[$aRecord['ФизЛицо_Key']]) ) {
                            continue;
                        }

                        $sCode = isset($income_codes[$aRecord['КодДохода_Key']]) ? $income_codes[$aRecord['КодДохода_Key']] : null;
                        $rows[] = [
                            ($sCode != 2000)? date("Y-m-d", strtotime($aRecord['ДатаПолученияДохода'])) : date("Y-m-d", strtotime($aRecord['Period'])),
                            $aRecord['СуммаДохода'],
                            $sCode,
                            isset($ded_codes[$aRecord['КодВычета_Key']]) ? $ded_codes[$aRecord['КодВычета_Key']] : null,
                            $aRecord['СуммаВычета'],
                            $this->active_pers_private[$aRecord['ФизЛицо_Key']]['id'],
                        ];
                    }
                }

                if (isset($rows) and count($rows)) {
                    Yii::$app->db
                        ->createCommand()
                        ->batchInsert('ndfl_income', [
                            'income_date',
                            'income_total',
                            'income_type',
                            'ded_type',
                            'ded_total',
                            'personal_id'
                        ], $rows)
                        ->execute();

                    unset($rows);
                    unset($ndfl_income);
                }
            }
        } else {
            Yii::error('Bad request odata client', 'import');
            throw new HttpException(403, 'Bad request odata client');
        }
    }


    /**
     * Обновляем вычеты для сотрудников
     * @param $clear
     */
    private function odata_sotr_ndfl_ded_update($clear)
    {
        if ($clear) {
            $old_a = NdflDed::find()
                ->joinWith('personal')
                ->where(['personal.organization_id' => $this->oOrganizationModel->id])
                ->asArray()
                ->all();
            NdflDed::deleteAll(['in','id', array_values( $old_a)]);
            unset($old_a);
        }

        NdflDed::deleteAll(['personal_id' => null]);

        $income_codes = [];
        $ded_codes = [];

        $income_types =  $this->odata_client->{'Catalog_ДоходыНДФЛ'}->get(null, "DeletionMark eq false");

        if ($this->odata_client->request_ok) {
            if(count($income_types['value'])) {
                foreach ($income_types['value'] as $o_code) {
                    $income_codes[$o_code['Ref_Key']] = $o_code['Code'];
                }
            }
        } else {
            Yii::error('Bad request in odata client in "Catalog_ВидыДоходовНДФЛ"', 'import');
            throw new HttpException(403, 'Bad request in odata client in "Catalog_ВидыДоходовНДФЛ"');
        }

        $ded_types = $this->odata_client->{'Catalog_ВычетыНДФЛ'}->get(null, "DeletionMark eq false");
        if ($this->odata_client->request_ok) {
            if(count($ded_types['value'])) {
                foreach ($ded_types['value'] as $o_code) {
                    $ded_codes[$o_code['Ref_Key']] = $o_code['КодДляОтчетности2016'];
                }
            }
        } else {
            Yii::error('Bad request in odata client in "Catalog_ВидыДоходовНДФЛ"', 'import');
            throw new HttpException(403, 'Bad request in odata client in "Catalog_ВидыДоходовНДФЛ"');
        }

        foreach ($this->active_pers as $pers) {

//            $filter = "ФизическоеЛицо_Key eq guid'".$pers['private_guid']."' and Active eq true";
            $ndfl_ded = $this->odata_client->{'AccumulationRegister_НДФЛПредоставленныеСтандартныеВычетыФизЛиц'}->get(null/*, $filter*/);
            if ($this->odata_client->request_ok) {
                if(count($ndfl_ded['value'])) {

                    $rows = [];
                    foreach ($ndfl_ded['value'] as $o_ded) {
                        foreach ($o_ded['RecordSet'] as $aRecord) {

                            if ($aRecord['ФизЛицо_Key'] != $pers['private_guid'] || !$aRecord['Active']) {
                                continue;
                            }

                            $rows[] = [
                                date("Y-m-d", strtotime($aRecord['Period'])),
                                $aRecord['ПримененныйВычет'],
                                isset($ded_codes[$aRecord['КодВычета_Key']]) ? $ded_codes[$aRecord['КодВычета_Key']] : null,
                                $this->active_pers_private[$aRecord['ФизЛицо_Key']]['id'],
                            ];

                        }
                    }

                    if (isset($rows) and count($rows)) {
                        Yii::$app->db
                            ->createCommand()
                            ->batchInsert('ndfl_ded', [
                                'ded_date',
                                'ded_total',
                                'ded_type',
                                'personal_id'
                            ], $rows)
                            ->execute();
                    }

                }

            } else {
                Yii::error('Bad request odata client in "AccumulationRegister_ПредоставленныеСтандартныеИСоциальныеВычетыНДФЛ_RecordType"', 'import');
                throw new HttpException(403, 'Bad request odata client in "AccumulationRegister_ПредоставленныеСтандартныеИСоциальныеВычетыНДФЛ_RecordType"');
            }

        }
    }


}