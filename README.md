# ywm-monitoring
<p>Скрипт-сборщик данных из API Мониторинг поисковых запросов Яндекс</p>

<p>Документация API:</p>

<p>https://yandex.ru/dev/webmaster/doc/dg/reference/host-query-analytics.html</p>

<p>&nbsp;</p>

<p>Скрипт работает на версии Python 3.10.12 и БД 10.6.12-MariaDB. На других не тестировал.</p>

<p>&nbsp;</p>

<p>Используемые модули:</p>

<ul>
	<li>requests</li>
	<li>pandas</li>
	<li>pytz</li>
	<li>mariadb</li>
</ul>

<p>Для установки mariadb часто требуется предварительно установить зависимость:</p>

<p>sudo apt-get install libmariadb3 libmariadb-dev</p>

<p>&nbsp;</p>

<p>Для работы требуется получить токен:</p>

<p>https://yandex.ru/dev/id/doc/ru/register-client</p>

