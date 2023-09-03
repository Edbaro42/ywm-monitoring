# ywm-monitoring
Скрипт-сборщик данных из API Мониторинг поисковых запросов Яндекс
Докуменатция API:
https://yandex.ru/dev/webmaster/doc/dg/reference/host-query-analytics.html


Скрипт работает на версии Python 3.10.12 и БД 10.6.12-MariaDB. На других не тестировал.

Используемые модули:
requests \n
pandas
pytz
mariadb
Для установки mariadb часто требуется предварительно установить зависимость:
sudo apt-get install libmariadb3 libmariadb-dev

Для работы требуется получить токен:
https://yandex.ru/dev/id/doc/ru/register-client
