<?php

namespace console\modules\import\components\import;

use common\models\Report;
use console\modules\import\components\import\interfaces\IImport;

use Yii;
use yii\db\Exception;
use yii\web\HttpException;

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
use common\models\RetentionAdditional;
use common\models\Contribution;

use Kily\Tools1C\OData\Client;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class Import3_0 extends Import implements IImport
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
        $this->odata_get_server_orgs(1); // 1 - только активные (те, которые нужны для импорта)
        $this->odata_orgs_update();
        return true;
    }

    /**
     * Обновляем список организации с сервера
     * @param bool $clear - удалить все организации, true - да
     * @return mixed
     */
    public function odata_orgs_list_update($clear)
    {
        if ($clear) {
            Organization::deleteAll(['odata_server_id' => $this->oOdataServerModel->id]);
            Yii::info("All organizations which belong to ".$this->oOdataServerModel->id.' server_id were deleted from db.', 'import');
        }
        $odata_orgs = $this->odata_client->{'Catalog_Организации'}->get(null);

        if ($this->odata_client->request_ok) {

            if (isset($odata_orgs['value'])) {
                if (count($odata_orgs['value'])) {

                    foreach ($odata_orgs['value'] as $odata_org) {
                        $org_id = $odata_org['Ref_Key'];
                        $db_organization = Organization::find()
                            ->where(['guid' => $org_id])
                            ->one();
                        if (!$db_organization) {
                            $db_organization = new Organization([
                                'guid' => $org_id,
                            ]);
                        }

                        //Основные реквизиты
                        $db_organization->name = $odata_org['НаименованиеСокращенное'];
                        $db_organization->inn = $odata_org['ИНН'];
                        $db_organization->ogrn = $odata_org['ОГРН'];

                        //Адресные данные
                        foreach ($odata_org['КонтактнаяИнформация'] as $item) {
                            if ($item['Тип'] == 'Адрес' && !$db_organization->adress_legal) {
                                $db_organization->adress_legal = $item['Представление'];
                            } elseif ($item['Тип'] == 'Телефон' && !$db_organization->phone) {
                                $db_organization->phone = $item['НомерТелефона'];
                            }
                        }

                        //Налоговоые реквизиты
                        if ($odata_org['РегистрацияВНалоговомОргане_Key']) {
                            $org_reg_nalog = $this->odata_client->{'Catalog_РегистрацииВНалоговомОргане'}->get($odata_org['РегистрацияВНалоговомОргане_Key']);

                            if ($this->odata_client->request_ok) {
                                $db_organization->kpp = $org_reg_nalog['КПП'];
                                $db_organization->oktmo = $org_reg_nalog['КодПоОКТМО'];
                            }
                        }

                        $db_organization->odata_server_id = $this->oOdataServerModel->id;
                        $db_organization->updated = date('Y-m-d H:i:s');
                        if (!$db_organization->save()) {
                            Yii::error('Organization was not saved...', 'import');
                            throw new Exception('Organization was not saved...', $db_organization->getErrors());
                        }

                        // Создаём финансовый отчёт в базе для каждой новой огрганизации:
                        $this->create_financial_report($db_organization->id);

                        unset($db_organization);
                    }

                }
            }

        } else {
            Yii::info('Bad request odata client "Catalog_Организации" '.$this->odata_client->error_message, 'import');
        }
        unset($odata_orgs);
        Yii::info('The list of organizations on server side of '.$this->oOdataServerModel->id.' server_id was updated: '.date("Y-m-d H:I:S").'.', 'import');
    }


    /*-------------------- Client-side functions: раньше были в common/models/Organization --------------------*/


    /**
     * Обновление организации в базе
     */
    public function odataUpdate()
    {
        $serv = OdataServer::find()
            ->where(['id' => $this->oOrganizationModel->odata_server_id])
            ->one();

        $this->odata_client = new Client($serv->odata_host, Yii::$app->params['odata_access_config']); // доступ в common/config/params-local.php
        if (!$this->odata_client) {
            Yii::error("OData Client object was not created on Client-side. Check access config in common/config/params-local.php", 'import');
            throw new HttpException(403, 'OData Client object was not created on Client-side. Check access config in common/config/params-local.php');
        }

        echo "\r\nStarted updating for organization id ".$this->oOrganizationModel->id."\r\n";

        echo "Started updating of personal data...\r\n";
        //обновляем сотрудников
        $this->odata_sotr_update(false);
        echo "...Ended updating of personal data\r\n";

        //обновляем активных сотрудников
        echo "Started updating of active personal data...\r\n";
        $this->odata_get_active_personals();
        echo "...Ended updating of active personal data\r\n";

        //обновляем адреса сотрудников
        echo "Started updating of personal's addresses...\r\n";
        $this->odata_sotr_adress_update(false);
        echo "...Ended updating of personal's addresses\r\n";

        //обновляем отпуска сотрудников
        echo "Started updating of personal's vacations...\r\n";
        $this->odata_sotr_vacancy_update(true);
        echo "...Ended updating of personal's vacations\r\n";

        //Обновляем платежи сотрудников
        echo "Started updating of personal's payments...\r\n";
        $this->odata_sotr_payments_update(true);
        echo "...Ended updating of personal's payments\r\n";

        //Обновляем начисления сотрудников
        echo "Started updating of personal's accruals...\r\n";
        $this->odata_sotr_accruals_update(true);
        echo "...Ended updating of personal's accruals\r\n";

        //Обновляем удержания сотрудников
        echo "Started updating of personal's retentions...\r\n";
        $this->odata_sotr_retention_update(true);
        echo "...Ended updating of personal's retentions...\r\n";

        //Обновляем доходы для ндфл сотрудников
        echo "Started updating of personal's ndfl and incomes...\r\n";
        $this->odata_sotr_ndfl_income_update(true);
        echo "...Ended updating of personal's ndfl and incomes\r\n";

        //Обновляем вычеты для сотрудников
        echo "Started updating of personal's ndfl and ded...\r\n";
        $this->odata_sotr_ndfl_ded_update(true);
        echo "...Ended updating of personal's ndfl and ded\r\n";

        echo "Started accumulation of fix payments - the last func...\r\n";
        $this->odata_fix_payments();
        echo "...Ended accumulation of fix payments\r\n";


        echo "Started getting additional retentions...\r\n";
        $this->odata_retention_additional(true); // только в импорте 3.0+
        echo "...Ended getting additional retentions\r\n";

        echo "Started getting contributions of organization...\r\n";
        $this->odata_contribution_update(true); // только в импорте 3.0 - взносы организации за сотрудников
        echo "...Ended getting contributions of organization\r\n";

        // Если персон удалили при импорте
        echo "Started users updating...\r\n";
        $this->update_users();
        echo "...Ended users updating\r\n";

        echo "Started updating statements,tabels,department_branch...\r\n";
        $this->update_statements();
        $this->update_tabels();
        $this->update_personal_department_branch();
        echo "...Ended updating statements,tabels,department_branch\r\n";

        echo "\r\n\...Ended of import_v3.0 for organization id ".$this->oOrganizationModel->id."\r\n";
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
            Yii::info('All personal which belongs to '.$this->oOrganizationModel->id.' organization_id was deleted from db.', 'import');
        }

        /*Собираем идентификатор паспорта физлица
        @todo добавить иссключение ошибки поиска
        */
        $doc_pasport = $this->odata_client->{'Catalog_ВидыДокументовФизическихЛиц'}->get(null, "Description eq 'Паспорт гражданина РФ'");
        $pasport_id = $doc_pasport['value'][0]['Ref_Key'];

        /*
        * Собираем текущие кадровые данные сотрудников организации
        */
        $departments_arr = [];
        $current_post_odata = $this->odata_client->{'Catalog_Должности'}->get(null);

        if ($this->odata_client->request_ok) {

            if (isset($current_post_odata['value'])) {
                if (count($current_post_odata['value'])) {
                    foreach ($current_post_odata['value'] as $post_data) {
                        $current_post_arr[$post_data['Ref_Key']] = $post_data;
                    }
                }
            }
        } else {
            Yii::error('Bad request odata client in "Catalog_Должности"', 'import');
            throw new HttpException(403, 'Bad request odata client in "Catalog_Должности"');
        }

        /*
         * Собираем подразделения организации
         */
        $departments_arr = [];
        $dep_odata = $this->odata_client->{'Catalog_ПодразделенияОрганизаций'}->get(null, "ГоловнаяОрганизация_Key eq guid'".$this->oOrganizationModel->guid."'");

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
        $departments_arr = [];
        $current_cadr_odata = $this->odata_client->{'InformationRegister_ТекущиеКадровыеДанныеСотрудников'}->get(null,"ГоловнаяОрганизация_Key eq guid'".$this->oOrganizationModel->guid."'");

        if ($this->odata_client->request_ok) {
            if (isset($current_cadr_odata['value'])) {
                if (count($current_cadr_odata['value'])) {
                    foreach ($current_cadr_odata['value'] as $pers_cadr_data) {
                        $current_cadr_arr[$pers_cadr_data['Сотрудник_Key']] = $pers_cadr_data;
                    }
                }
            }
        } else {
            Yii::error('Bad request odata client in "InformationRegister_ТекущиеКадровыеДанныеСотрудников"', 'import');
            throw new HttpException(403, 'Bad request odata client in "InformationRegister_ТекущиеКадровыеДанныеСотрудников"');
        }

        //Получаем оклад сотрудников
        $sotr_salary = $this->odata_client->{'Document_ПриемНаРаботу'}->get(null, "Организация_Key eq guid'".$this->oOrganizationModel->guid."'");

        if ($this->odata_client->request_ok) {
            if (isset($sotr_salary['value'])) {
                if (count($sotr_salary['value'])) {
                    foreach ($sotr_salary['value'] as $pers_salary) {
                        $sotr_salary_arr[$pers_cadr_data['Сотрудник_Key']] = $pers_salary;
                    }
                }
            }
        } else {
            Yii::error('Bad request odata client in "Document_ПриемНаРаботу"', 'import');
            throw new HttpException(403, 'Bad request odata client in "Document_ПриемНаРаботу"');
        }

        //Получаем кадровые переводы по организации
        $sotr_transfer = $this->odata_client->{'Document_КадровыйПеревод'}->get(null, "Организация_Key eq guid'".$this->oOrganizationModel->guid."'");
        if ($this->odata_client->request_ok) {

            if (isset($sotr_transfer['value'])) {
                if (count($sotr_transfer['value'])) {
                    foreach ($sotr_transfer['value'] as $pers_transfer) {
                        $sotr_transfer_arr[$pers_cadr_data['Сотрудник_Key']] = $pers_transfer;
                    }
                }
            }
        } else {
            Yii::info('No "Document_КадровыйПеревод" for '.$this->oOrganizationModel->guid.' organization', 'import');
//            throw new HttpException(403, 'Bad request odata client in "Document_КадровыйПеревод"');
        }

        /*
         * Получаем список сотрудников - Основной Цикл
         */
        $sotr = $this->odata_client->{'Catalog_Сотрудники'}->get(null, "ГоловнаяОрганизация_Key eq guid'".$this->oOrganizationModel->guid."' and ВАрхиве eq false and DeletionMark eq false");

        if (isset($sotr['value'])) {
            if (count($sotr['value'])) {

                foreach ($sotr['value'] as $skey => $svalue) {
                    //$time_start = microtime(true);

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
                        ($current_cadr_arr[$db_person->guid]['ДатаУвольнения'] != '0001-01-01T00:00:00' ||
                            $current_cadr_arr[$db_person->guid]['ДатаПриема'] == '0001-01-01T00:00:00')
                    ) {
                        var_dump('continued '.$db_person->id."\r\n");
                        continue;
                    }

                    $db_person->private_guid = $svalue['ФизическоеЛицо_Key'];

                    if( isset($current_cadr_arr[ $db_person->guid])) {
                        if( isset($current_cadr_arr[ $db_person->guid]['ТекущееПодразделение_Key'])) {
                            if (isset($departments_arr[$current_cadr_arr[$db_person->guid]['ТекущееПодразделение_Key']])) {
                                $db_person->department = $departments_arr[$current_cadr_arr[$db_person->guid]['ТекущееПодразделение_Key']];
                            }
                        }

                        if (isset($current_post_arr[$current_cadr_arr[$db_person->guid]['ТекущаяДолжность_Key']])) {
                            $db_person->post = $current_post_arr[$current_cadr_arr[$db_person->guid]['ТекущаяДолжность_Key']]["Description"];
                            $db_person->aproove_date =  $current_cadr_arr[$db_person->guid]['ДатаПриема'];
                        }
                    }

                    //$time_start = microtime(true);
                    $sotr_data = $this->odata_client->{'Catalog_ФизическиеЛица'}->get($db_person->private_guid);
                    //echo 'Total execution time in seconds: ' . (microtime(true) - $time_start)."\r\n";

                    if ($this->odata_client->request_ok) {

                        $db_person->first_name = $sotr_data['Имя'];
                        $db_person->surname = $sotr_data['Фамилия'];
                        $db_person->last_name = $sotr_data['Отчество'];
                        $db_person->birth_date = $sotr_data['ДатаРождения'];

                        $db_person->organization_id = $this->oOrganizationModel->id;

                        if(isset($sotr_salary_arr[$db_person->guid])) {
                            $db_person->salary = $sotr_salary_arr[$db_person->guid]['СовокупнаяТарифнаяСтавка'];
                        }

                        if(isset($sotr_transfer_arr[$db_person->guid])) {
                            $db_person->salary = $sotr_transfer_arr[$db_person->guid]['СовокупнаяТарифнаяСтавка'];
                        }

                        $db_person->updated = date('Y-m-d H:i:s');
                        // Сохранение персоны в базе:
                        if ($db_person->save()) {

                            $this->active_pers[$db_person->guid] = [
                                'id' => $db_person->id,
                                'private_guid' => $db_person->private_guid
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
                                throw new Exception('СНИЛС персоны '.$db_person->id.' не сохранён', $snpfr->getErrors());
                            }
                        }

                        $documents = $this->odata_client->{'InformationRegister_ДокументыФизическихЛиц'}->get(null,"Физлицо_Key eq guid'".$svalue['ФизическоеЛицо_Key']."' & ВидДокумнта_Key eq guid'".$pasport_id."'");

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
                                $iFresh = strtotime($documents['value'][$i]['ДатаВыдачи']) > strtotime($documents['value'][$i-1]['ДатаВыдачи']) ? $i : ($i - 1);
                                $i++;
                            }

                            if (!$pass) {  // если нет паспорта, то создаём его для персоны
                                $pass = new Document([
                                    'guid' => $db_person->guid,
                                    'document_type' => 'Паспорт',
                                    'personal_id' => $db_person->id,
                                    'dval1' => isset($documents['value'][$iFresh]['Серия']) ? $documents['value'][$iFresh]['Серия'] : null,
                                    'dval2' => isset($documents['value'][$iFresh]['Номер']) ? $documents['value'][$iFresh]['Номер'] : null,
                                    'dval3' => isset($documents['value'][$iFresh]['ДатаВыдачи']) ? $documents['value'][$iFresh]['ДатаВыдачи'] : null,
                                    'dval4' => isset($documents['value'][$iFresh]['КемВыдан']) ? $documents['value'][$iFresh]['КемВыдан'] : null,
                                    'dval5' => isset($documents['value'][$iFresh]['КодПодразделения']) ? $documents['value'][$iFresh]['КодПодразделения'] : null,
                                ]);
                                Yii::info('Персоне с person.id '.$db_person->id.' добавлен новый паспорт.', 'import');
                            }

                            if ($pass->save()) { // Сохранение паспортных данных!
                                Yii::info('Персоне с person.id '.$db_person->id.' сохранены изменения в паспорте '.$pass->id.'. '.date('Y-m-d H:i:s'), 'import');
                            } else {
                                Yii::error('Passport of person_id '.$db_person->id.' was not saved.', 'import');
                                throw new HttpException(403, 'Passport of person_id '.$db_person->id.' was not saved. '.print_r($pass->getErrors(), true) );
                            }
                            unset($pass);
                        } else {
                            Yii::error('Bad request odata client in "InformationRegister_ДокументыФизическихЛиц" passport updating', 'import');
                            throw new HttpException(403, 'Bad request odata client in "InformationRegister_ДокументыФизическихЛиц" passport updating');
                        }

                        unset($documents);
                        unset($db_person);
                        //echo 'Pers time: ' . (microtime(true) - $time_start)."\r\n";
                    } else {
                        Yii::error('Bad request odata client "Catalog_ФизическиеЛица" '.$this->odata_client->error_message, 'import');
                        throw new HttpException(403, 'Bad request odata client "Catalog_ФизическиеЛица" '.$this->odata_client->error_message);
                    }


                }
            }
        }
        //echo "Memory before delete:".memory_get_usage() . "\r\n";
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
     * @return mixed
     */
    private function odata_sotr_adress_update($clear)
    {
        if (empty($this->active_pers_private)) {
            Yii::info('No active persons for updating in "odata_sotr_adress_update".', 'import');
            return;
        }

        if ($clear) {
            $old_p = PersonalAdres::find()
                ->joinWith('personal')
                ->where(['personal.organization_id' => $this->oOrganizationModel->id])
                ->asArray()
                ->all();
            PersonalAdres::deleteAll(['in', 'id', array_values($old_p)]);
        }

        $personal_odata = $this->odata_client->{'Catalog_ФизическиеЛица_КонтактнаяИнформация'}->get(null);

        if ($this->odata_client->request_ok) {
            if ($personal_odata['value']) {

                foreach ($personal_odata['value'] as $cvalue) {

                    if (!isset($this->active_pers_private[$cvalue['Ref_Key']])) {
                        continue;
                    }

                    if (isset($cvalue['Вид_Key'])) {

                        $adr = PersonalAdres::findOne([ 'personal_id' => $this->active_pers_private[$cvalue['Ref_Key']]['id'] ]);
                        if (!$adr) {
                            $adr = new PersonalAdres([ 'personal_id' => $this->active_pers_private[$cvalue['Ref_Key']]['id'] ]);
                        }

                        if (@$a = simplexml_load_string($cvalue["ЗначенияПолей"])) {
                            if (isset($a->Страна)) {
                                $adr->country = (string)$a->Страна;
                            }
                            if (isset($a->Состав->Состав->Город)) {
                                $adr->city = (string)$a->Состав->Состав->Город;
                            }
                            if (isset($a->Состав->Состав->НаселПункт)) {
                                $adr->locality = (string)$a->Состав->Состав->НаселПункт;
                            }

                            if (isset($a->Состав->Состав->СубъектРФ) && !isset($a->Состав->Состав->НаселПункт) && !isset($a->Состав->Состав->Город)) {
                                $adr->locality = (string)$a->Состав->Состав->СубъектРФ;
                            }
                            if (isset($a->Состав->Состав->Улица)) {
                                $adr->street = (string)$a->Состав->Состав->Улица;
                            }

                            if (isset($a->Состав->Состав->ДопАдрЭл)) {
                                foreach ($a->Состав->Состав->ДопАдрЭл as $dop_a) {
                                    if (isset($dop_a->Номер)) {
                                        switch ((string)$dop_a->Номер['Тип']) {
                                            case "2010":
                                                $adr->house_flat = (integer)$dop_a->Номер['Значение'];
                                                //var_dump((integer)$dop_a->Номер->Значение);
                                                break;
                                            case "1010":
                                                $adr->house_number = (string)$dop_a->Номер['Значение'];
                                                break;
                                            case "1050":
                                                $adr->house_block = (string)$dop_a->Номер['Значение'];
                                                break;
                                        }
                                    } elseif (isset($dop_a['ТипАдрЭл'])) {
                                        if ($dop_a['ТипАдрЭл'] == "10100000") {
                                            if (is_int($dop_a['Значение'])) {
                                                $adr->post_index = $dop_a['Значение'];
                                            }
                                        }
                                    }
                                }
                            }
                            unset($a);

                        } else {

                            $lines = explode(PHP_EOL, $cvalue["ЗначенияПолей"]);
                            if (count($lines)) {
                                foreach ($lines as $l) {
                                    $line = explode("=", $l);
                                    if (isset($line[1])) {
                                        switch ($line[0]) {
                                            case "Индекс":
                                                $adr->post_index = $line[1];
                                                break;
                                            case "Дом":
                                                $adr->house_number = $line[1];
                                                break;
                                            case "Корпус":
                                                $adr->house_block = $line[1];
                                                break;
                                            case "Квартира":
                                                $adr->house_flat = $line[1];
                                                break;
                                        }
                                    }
                                }
                            }
                            unset($lines);
                        }

                        if (!$adr->save()) {
                            Yii::error('Save address error', 'import');
                            throw new Exception('Save address error', $adr->getErrors());
                        }
                        unset($adr);
                    }
                }
            }
        } else {
            Yii::error('Bad request odata client "Catalog_ФизическиеЛица"', 'import');
            var_dump($this->odata_client->getErrorMessage());
            throw new HttpException(403, 'Bad request odata client "Catalog_ФизическиеЛица"');
        }
    }

    /**
     * Обновляем отпуска сотрудников
     * @param $clear
     * @return mixed
     */
    private function odata_sotr_vacancy_update($clear)
    {
        if ($clear) {
            $old_p = PersonalVacation::find()
                ->joinWith('personal')
                ->where(['personal.organization_id' => $this->oOrganizationModel->id])
                ->asArray()
                ->all();
            PersonalVacation::deleteAll(['in', 'id', array_values($old_p)]);
        }

        $vacations = $this->odata_client->{'AccumulationRegister_ФактическиеОтпуска_RecordType'}->get(null, "Компенсация eq false"); //,"Компенсация eq false");

        if ($this->odata_client->request_ok ) {

            if (isset($vacations['value'])) {
                if (count($vacations['value'])) {
                    foreach ($vacations['value'] as $o_vacation) {

                        if (!isset($this->active_pers[$o_vacation['Сотрудник_Key']])) {
                            continue;
                        }

                        if (isset($o_vacation['ДатаНачала'])) {
                            $db_vacation = new PersonalVacation([
                                'date_start' => $o_vacation['ДатаНачала'],
                                'date_end' => $o_vacation['ДатаОкончания'],
                                'vacation_count' => intval($o_vacation['Количество']),
                                'personal_id' => $this->active_pers[$o_vacation['Сотрудник_Key']]['id']
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
                Yii::info('Empty vacations in "AccumulationRegister_ФактическиеОтпуска_RecordType" ...', 'import');
            }
        } else {
            Yii::error('Bad request in "odata_sotr_vacancy_update" of odata Client "AccumulationRegister_ФактическиеОтпуска_RecordType"', 'import');
            throw new HttpException(403, 'Bad request in "odata_sotr_vacancy_update" of odata Client "AccumulationRegister_ФактическиеОтпуска_RecordType"');
        }


        return true;
    }

    /**
     * Обновляем платежи сотрудников
     * @param bool $clear - удяалять ли всех старых до импорта, true - да
     * @return mixed
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
        }

        //Собираем способы выплаты(аванс, зарплата)
        $payment_types_arr = [];
        $payment_types = $this->odata_client->{'Catalog_СпособыВыплатыЗарплаты'}->get();

        if ($this->odata_client->request_ok) {
            if (count($payment_types['value'])) {

                foreach ($payment_types['value'] as $ptype) {
                    $payment_types_arr[$ptype['Ref_Key']] = $ptype['Description'];
                }
            }
        } else {
            Yii::error('Bad request in "Catalog_СпособыВыплатыЗарплаты" odata client "odata_sotr_payments_update"', 'import');
            throw new HttpException(403, 'Bad request in "Catalog_СпособыВыплатыЗарплаты" odata client "odata_sotr_payments_update"');
        }

        $ved_arr = [
            'ВедомостьНаВыплатуЗарплатыПеречислением',
            'ВедомостьНаВыплатуЗарплатыРаздатчиком',
            'ВедомостьНаВыплатуЗарплатыВБанк',
            'ВедомостьНаВыплатуЗарплатыВКассу',
        ];

        foreach ($ved_arr as $va) {

            $payments = $this->odata_client->{'Document_' . $va}->get(null, "Организация_Key eq guid'".$this->oOrganizationModel->guid."' and DeletionMark eq false and  Posted eq true");//null, "Организация_Key eq guid'".$a_id."' ".$filter);
            if ($this->odata_client->request_ok) {

                if (count($payments['value'])) {
                    foreach ($payments['value'] as $o_payment) {

                        if (isset($o_payment['Зарплата'])) {
                            foreach ($o_payment["Зарплата"] as $o_payment_entry) {

                                if (isset($this->active_pers[$o_payment_entry['Сотрудник_Key']])) {
                                    $db_payment = new Payment();

                                    $db_payment->date = date("Y-m-d H:i:s", strtotime($o_payment['Date']));
                                    $db_payment->payment_period = date("Y-m-d H:i:s", strtotime($o_payment['Date']));
                                    $db_payment->payment_type = $payment_types_arr[$o_payment['СпособВыплаты_Key']];
                                    $db_payment->payment_total = strval($o_payment_entry['КВыплате']);
                                    $db_payment->personal_id = isset($this->active_pers[$o_payment_entry['Сотрудник_Key']]) ? $this->active_pers[$o_payment_entry['Сотрудник_Key']]['id'] : null;
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
        unset($payment_types_arr);
        unset($ved_arr);
    }

    /**
     * Обновляем начисления сотрудников
     * @param $clear
     * @return mixed
     */
    private function odata_sotr_accruals_update($clear)
    {
        $rows = [];
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
        $accrual_types_arr = [];
        $accrual_types = $this->odata_client->{'ChartOfCalculationTypes_Начисления'}->get();

        if ($this->odata_client->request_ok) {

            if (count($accrual_types['value'])) {
                foreach ($accrual_types['value'] as $atype) {
                    $accrual_types_arr[$atype['Ref_Key']] = $atype['Description'];
                }
            }
        } else {
            Yii::error('Bad request in "odata_sotr_accruals_update" for "ChartOfCalculationTypes_Начисления" to odata client', 'import');
            throw new HttpException(403, 'Bad request in "odata_sotr_accruals_update" for "ChartOfCalculationTypes_Начисления" to odata client');
        }

        $filter ='';
        $filter .= "Организация_Key eq guid'".$this->oOrganizationModel->guid."' and Active eq true";
        $o_accruals = $this->odata_client->{'CalculationRegister_Начисления_RecordType'}->get(null, $filter);

        if ($this->odata_client->request_ok) {

            if (count($o_accruals['value'])) {
                foreach ($o_accruals['value'] as $o_accrual) {
                    if (!isset($this->active_pers[$o_accrual['Сотрудник_Key']]) || !$o_accrual['Результат']) {
                        continue;
                    }

                    switch($accrual_types_arr[$o_accrual['CalculationType_Key']]) {
                        case 'Оплата по окладу':
                            $iSort = 10;
                            break;
                        case 'Премия ежемесячная':
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
                        $o_accrual['ОтработаноДней'],
                        $o_accrual['ОтработаноЧасов'],
                        $o_accrual['Результат'],
                        $this->active_pers[$o_accrual['Сотрудник_Key']]['id'],
                        $accrual_types_arr[$o_accrual['CalculationType_Key']],
                        date("Y-m-d",strtotime($o_accrual['RegistrationPeriod'])),
                        $iSort,
                    ];
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
                unset($time_acc);
                unset($rows);
            }
        } else {
            Yii::error('Bad request in "odata_sotr_accruals_update" for "CalculationRegister_Начисления_RecordType" to odata client', 'import');
            throw new HttpException(403, 'Bad request in "odata_sotr_accruals_update" for "CalculationRegister_Начисления_RecordType" to odata client');
        }
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
        }
        unset($old_a);

        //Собираем типы начислений
        $retention_types_arr = [];
        $retention_types = $this->odata_client->{'ChartOfCalculationTypes_Удержания'}->get();

        if ($this->odata_client->request_ok) {

            if (count($retention_types['value'])) {
                foreach ($retention_types['value'] as $rtype) {
                    $retention_types_arr[$rtype['Ref_Key']] = $rtype['Description'];
                }
            }
        } else {
            Yii::error('Bad request in "odata_sotr_retention_update" for "ChartOfCalculationTypes_Удержания"', 'import');
            throw new HttpException(403, 'Bad request in "odata_sotr_retention_update" for "ChartOfCalculationTypes_Удержания"');
        }

        $filter = "Организация_Key eq guid'".$this->oOrganizationModel->guid ."' and ГруппаНачисленияУдержанияВыплаты eq 'Удержано'";

        $time_ret = microtime(true);
        $o_retentions = $this->odata_client->{'AccumulationRegister_НачисленияУдержанияПоСотрудникам_RecordType'}->get(null, $filter);
        $rows = [];

        if ($this->odata_client->request_ok) {

            if (count($o_retentions['value'])) {
                $rows = [];

                foreach ($o_retentions['value'] as $o_retention) {
                    if(!isset($this->active_pers[$o_retention['Сотрудник_Key']])) Continue;
                    if ($o_retention['ГруппаНачисленияУдержанияВыплаты'] != 'Удержано') Continue;
                    $rows[]=[
                        $o_retention['Сумма'],
                        $this->active_pers[$o_retention['Сотрудник_Key']]['id'],
                        date("Y-m-d", strtotime($o_retention['Period'])),
                        (strpos($o_retention['НачислениеУдержание'], '-'))?$retention_types_arr[$o_retention['НачислениеУдержание']]:$o_retention['НачислениеУдержание'],
                    ];
                }
            }
        } else {
            Yii::error('Bad request in "odata_sotr_retention_update" for "AccumulationRegister_НачисленияУдержанияПоСотрудникам_RecordType"', 'import');
            throw new HttpException(403, 'Bad request in "odata_sotr_retention_update" for "AccumulationRegister_НачисленияУдержанияПоСотрудникам_RecordType"');
        }

        unset($o_retentions);
        if (isset($rows) and count($rows)) {
            \Yii::$app->db
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
     * @return mixed
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
        }

        NdflIncome::deleteAll(['personal_id' => null]);
        unset($old_a);

        $income_codes = [];
        $ded_codes = [];

        $income_types = $this->odata_client->{'Catalog_ВидыДоходовНДФЛ'}->get(null, "DeletionMark eq false");

        if ($this->odata_client->request_ok) {
            if (count($income_types['value'])) {
                foreach ($income_types['value'] as $o_code) {
                    $income_codes[$o_code['Ref_Key']] = $o_code['Code'];
                }
            }
        } else {
            Yii::error('Bad request in odata client in "Catalog_ВидыДоходовНДФЛ"', 'import');
            throw new HttpException(403, 'Bad request in odata client in "Catalog_ВидыДоходовНДФЛ"');
        }

        $ded_types = $this->odata_client->{'Catalog_ВидыВычетовНДФЛ'}->get(null, "DeletionMark eq false");

        if ($this->odata_client->request_ok) {

            if (count($ded_types['value'])) {
                foreach ($ded_types['value'] as $o_code) {
                    $ded_codes[$o_code['Ref_Key']] = $o_code['КодПрименяемыйВНалоговойОтчетностиС2016Года'];
                }
            }
        } else {
            Yii::error('Bad request in odata client in "Catalog_ВидыВычетовНДФЛ"', 'import');
            throw new HttpException(403, 'Bad request in odata client in "Catalog_ВидыВычетовНДФЛ"');
        }


        $ndfl_income = $this->odata_client->{'AccumulationRegister_СведенияОДоходахНДФЛ'}->get(null);
        if ($this->odata_client->request_ok) {

            $aIncomeData = [];
            foreach($ndfl_income['value'] as $aRecord) {
                foreach ($aRecord['RecordSet'] as $aIncome) {

                    if ( !isset($this->active_pers[$aIncome['Сотрудник_Key']]) || !$aIncome['Active'] ) {
                        continue;
                    }

                    $sCode = isset($income_codes[$aIncome['КодДохода_Key']]) ? $income_codes[$aIncome['КодДохода_Key']] : null;
                    $aIncomeData[] = [
                        $this->active_pers[$aIncome['Сотрудник_Key']]['id'],
                        $sCode,
                        isset($ded_codes[$aIncome['КодВычета_Key']]) ? $ded_codes[$aIncome['КодВычета_Key']] : null,
                        $aIncome['СуммаДохода'],
                        $aIncome['СуммаВычета'],
                        ($sCode != 2000)? date("Y-m-d", strtotime($aIncome['ДатаПолученияДохода'])) : date("Y-m-d", strtotime($aIncome['Period'])),
                    ];

                }
            }

            if (count($aIncomeData)) {
                Yii::$app->db
                    ->createCommand()
                    ->batchInsert(NdflIncome::tableName(), [
                        'personal_id',
                        'income_type',
                        'ded_type',
                        'income_total',
                        'ded_total',
                        'income_date',
                    ], $aIncomeData)
                    ->execute();
                unset($aIncomeData);
            }

        } else {
            Yii::error('Bad request odata client', 'import');
            throw new HttpException(403, 'Bad request odata client');
        }

    }

    /**
     * Обновляем вычеты для сотрудников
     * @param $clear
     * @return mixed
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
        }

        NdflDed::deleteAll(['personal_id' => null]);

        $income_codes = [];
        $ded_codes = [];

        $income_types =  $this->odata_client->{'Catalog_ВидыДоходовНДФЛ'}->get(null, "DeletionMark eq false");

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

        $ded_types = $this->odata_client->{'Catalog_ВидыВычетовНДФЛ'}->get(null, "DeletionMark eq false");
        if ($this->odata_client->request_ok) {
            if(count($ded_types['value'])) {
                foreach ($ded_types['value'] as $o_code) {
                    $ded_codes[$o_code['Ref_Key']] = $o_code['КодПрименяемыйВНалоговойОтчетностиС2016Года'];
                }
            }
        } else {
            Yii::error('Bad request in odata client in "Catalog_ВидыВычетовНДФЛ"', 'import');
            throw new HttpException(403, 'Bad request in odata client in "Catalog_ВидыВычетовНДФЛ"');
        }


        //my:
        $ndfl_ded = $this->odata_client->{'AccumulationRegister_ПредоставленныеСтандартныеИСоциальныеВычетыНДФЛ'}->get(null);
        if ($this->odata_client->request_ok) {

            $aDataDed = [];
            foreach($ndfl_ded['value'] as $aRecord) {
                foreach ($aRecord['RecordSet'] as $aDed) {

                    if (!$aDed['Active']) {
                        continue;
                    }

                    // todo: Неизвестно в каком ключе вычеты:
                    if ( isset($this->active_pers[$aDed['ФизическоеЛицо_Key']]) ) {
                        $personalId = $this->active_pers[$aDed['ФизическоеЛицо_Key']]['id'];
                    } elseif ( isset($this->active_pers_private[$aDed['ФизическоеЛицо_Key']]) ) {
                        $personalId = $this->active_pers_private[$aDed['ФизическоеЛицо_Key']]['id'];
                    } else {
                        continue;
                    }

                    $aDataDed[] = [
                        $personalId,
                        date("Y-m-d", strtotime($aDed['Period'])),
                        $aDed['Сумма'],
                        isset($ded_codes[$aDed['КодВычета_Key']]) ? $ded_codes[$aDed['КодВычета_Key']] : null,
                    ];

                }
            }

            if (count($aDataDed)) {
                Yii::$app->db
                    ->createCommand()
                    ->batchInsert(NdflDed::tableName(), [
                        'personal_id',
                        'ded_date',
                        'ded_total',
                        'ded_type',
                    ], $aDataDed)
                    ->execute();
                unset($aDataDed);
            }


        } else {
            Yii::error('Bad request odata client in "AccumulationRegister_ПредоставленныеСтандартныеИСоциальныеВычетыНДФЛ"', 'import');
            throw new HttpException(403, 'Bad request odata client in "AccumulationRegister_ПредоставленныеСтандартныеИСоциальныеВычетыНДФЛ"');
        }

    }


    /**
     * Вытягиваем из 1С дополнительные расходы, не проведённые в налоговых документах: гостиница, билеты, бензин и моб.связь
     * @param $clear - если удаление персон
     * @throws Exception
     * @throws HttpException
     */
    private function odata_retention_additional($clear)
    {
        if ($clear) {
            $old_a = RetentionAdditional::find()
                ->joinWith('personal')
                ->where(['personal.organization_id' => $this->oOrganizationModel->id])
                ->asArray()
                ->all();
            RetentionAdditional::deleteAll(['in', 'id', array_values($old_a)]);
        }

        //Собираем типы начислений
        $retention_types_arr = [];
        $retention_types = $this->odata_client->{'Document_Командировка_ДополнительныеРеквизиты'}->get(null);

        if ($this->odata_client->request_ok) {

            if (count($retention_types['value'])) {
                foreach ($retention_types['value'] as $rtype) {
                    $propertyKey = $rtype['Свойство_Key'];
                    switch($propertyKey) {
                        case '39aafae7-0cd0-11e8-8104-00155d0f6502':
                            $sDescription = 'Расходы на гостиницу';
                            break;
                        case '44438d91-0cd0-11e8-8104-00155d0f6502':
                            $sDescription = 'Расходы на топливо';
                            break;
                        case '4b3b7807-0cd0-11e8-8104-00155d0f6502':
                            $sDescription = 'Расходы на проезд';
                            break;
                        case '52fe05f7-0cd0-11e8-8104-00155d0f6502':
                            $sDescription = 'Расходы на мобильную связь';
                            break;
                        case 'bd9abe28-1251-11e8-8104-00155d0f6502':
                            $sDescription = 'Суточные расходы'; // это вместо "сверхнормативных суточных" которые облагаются ндфлом
                            break;
                    }
                    $retention_types_arr[$rtype['Ref_Key']][$propertyKey]['description'] = $sDescription;
                    $retention_types_arr[$rtype['Ref_Key']][$propertyKey]['total'] = $rtype["Значение"];
                }
            }
        } else {
            Yii::error('Bad request in "odata_retention_additional" for "Document_Командировка_ДополнительныеРеквизиты"', 'import');
            throw new HttpException(403, 'Bad request in "odata_retention_additional" for "Document_Командировка_ДополнительныеРеквизиты"');
        }

        $filter = "Организация_Key eq guid'".$this->oOrganizationModel->guid ."'";
        $o_retentions = $this->odata_client->{'Document_Командировка'}->get(null, $filter);
        $rows = [];

        if ($this->odata_client->request_ok) {

            if (count($o_retentions['value'])) {
                $rows = [];

                foreach ($o_retentions['value'] as $o_retention) {

                    if ( !isset($this->active_pers[$o_retention['Сотрудник_Key']]) or !isset($retention_types_arr[$o_retention['Ref_Key']]) ) {
                        continue;
                    }

                    foreach ($retention_types_arr[$o_retention['Ref_Key']] as $propertyKey => $aRetentionAddit) {

                        if (!isset($this->active_pers[$o_retention['Сотрудник_Key']])) {
                            continue;
                        }

                        $rows[] = [
                            $this->active_pers[$o_retention['Сотрудник_Key']]['id'],
                            $o_retention['Сотрудник_Key'],
                            $aRetentionAddit['total'],
                            $propertyKey,
                            $aRetentionAddit['description'],
                            date("Y-m-d", strtotime($o_retention['Date'])),
                        ];
                    }

                }
            }
        } else {
            Yii::error('Bad request in "odata_retention_additional" for "Document_Командировка"', 'import');
            throw new HttpException(403, 'Bad request in "odata_retention_additional" for "Document_Командировка"');
        }

        unset($o_retentions);
        if (isset($rows) and count($rows)) {
            Yii::$app->db
                ->createCommand()
                ->batchInsert(RetentionAdditional::tableName(), [
                    'personal_id',
                    'personal_guid',
                    'total',
                    'property_ref_key',
                    'description',
                    'period',
                ], $rows)
                ->execute();
        }
        unset($retention_types_arr);
    }

    public function odata_contribution_update($clear)
    {
        if ($clear) {
            $old_a = Contribution::find()
                ->joinWith('personal')
                ->where(['personal.organization_id' => $this->oOrganizationModel->id])
                ->asArray()
                ->all();
            Contribution::deleteAll(['in', 'id', array_values($old_a)]);
        }

        $contributions = $this->odata_client->{'AccumulationRegister_ИсчисленныеСтраховыеВзносы'}->get(null);
        $contributionTypes = [
            'ПФРДоПредельнойВеличины',
            'ПФРСПревышения',
            'ФСС',
            'ФССНесчастныеСлучаи',
            'ФФОМС',
        ];

        if ($this->odata_client->request_ok) {

            if ($contributions['value']) {
                foreach ($contributions['value'] as $oContribution) {
                    foreach($oContribution["RecordSet"] as $aContributions) {

                        if (!isset($this->active_pers_private[$aContributions['ФизическоеЛицо_Key']])) {
                            continue;
                        }

                        foreach ($contributionTypes as $sType) {
                            $rows[] = [
                                $this->active_pers_private[$aContributions['ФизическоеЛицо_Key']]['id'],
                                $this->active_pers_private[$aContributions['ФизическоеЛицо_Key']]['guid'],
                                $aContributions[$sType],
                                $sType,
                                date("Y-m-d", strtotime($aContributions['Period'])),
                                date("Y-m-d", strtotime($aContributions['ДатаПолученияДохода'])),
                            ];
                        }

                    }
                }
            }

            if (isset($rows) and count($rows)) {
                Yii::$app->db
                    ->createCommand()
                    ->batchInsert(Contribution::tableName(), [
                        'personal_id',
                        'personal_guid',
                        'total',
                        'description',
                        'period',
                        'receive_date',
                    ], $rows)
                    ->execute();
            }

        } else {
            Yii::error('Bad request in "odata_contribution_update" for "AccumulationRegister_ИсчисленныеСтраховыеВзносы"', 'import');
            throw new HttpException(403, 'Bad request in "odata_contribution_update" for "AccumulationRegister_ИсчисленныеСтраховыеВзносы"');
        }

    }


}