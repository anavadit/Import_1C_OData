<?php

namespace console\modules\import\components\import\interfaces;

interface IImport
{
    /*-------------------- Server-side functions: раньше были в common/models/OdataServer --------------------*/

    public function odata_update();

    /**
     * Обновляем список организации с сервера
     * @param $clear
     * @return mixed
     */
    public function odata_orgs_list_update($clear);

    /**
     * Получаем список текущих активных организаций
     * @param $active
     * @return mixed
     */
    public function odata_get_server_orgs($active);


    /**
     * Закидываем в очередь сообщения о необходимости обновления организаций
     * @return mixed
     */
    public function odata_orgs_update();


    /*-------------------- Client-side functions: раньше были в common/models/Organization --------------------*/


    /**
     * Обновление организации в базе
     * @return mixed
     */
    public function odataUpdate();

}