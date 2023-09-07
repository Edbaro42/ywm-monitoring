<p># ywm-monitoring</p>

<p>Скрипт-сборщик данных из API Мониторинг поисковых запросов Яндекс</p>

<p>Документация API:</p>

<p>https://yandex.ru/dev/webmaster/doc/dg/reference/host-query-analytics.html</p>

<p>Скрипт работает на версии Python 3.10.12 и БД 10.6.12-MariaDB. На других не тестировал.</p>

<p>Используемые модули:</p>

<ul>
	<li>requests</li>
	<li>pandas</li>
	<li>pytz</li>
	<li>mariadb</li>
</ul>

<p>Для установки mariadb часто требуется предварительно установить зависимость:</p>

<p>sudo apt-get install libmariadb3 libmariadb-dev</p>

<p>Для работы требуется получить токен:</p>

<p>https://yandex.ru/dev/id/doc/ru/register-client</p>

<h2>Изменения в ветке agrigate</h2>
<br>
<br>
<strong>За идею такой реализации выражаю огромную благодарность Сергею Николаевичу Вирясову!</strong>
<br>
<br>
<ol>
	<li>Полностью изменена логика сбора данных. Теперь данные собираются постранично. В БД появилась новая колонка - URL. Теперь можно смотреть статистику по запросам для каждого url отдельно.<br />
	Примечание: из-за возможности смены релевантных страниц для одного запроса в БД может быть несколько url. Это нормально.</li>
	<li>Для удобства работы с данными сделал автоматическое исправление для позиций. Если поисковый спрос больше нуля, то позиции присваивается 101. Это условие сделано из правила, что если запрос находится дальше 10 места, а пользователь не листал страницы СЕРПа, то в статистике позиции будет стоять 0. Однако раз есть спрос, значит пользователи вбивали запрос, просто наша страница оказалась сильно ниже.<br />
	Условие отменяется путем комментирования этих строк:<br />
	&nbsp; &nbsp; condition = (aggregated_data[&#39;DEMAND&#39;] &gt; 0)<br />
	&nbsp; &nbsp; aggregated_data.loc[condition, &#39;POSITION&#39;] = aggregated_data[&#39;POSITION&#39;].replace(0.00, 101.00)</li>
	<li>Стала работать региональность. Можно выбрать регион. Правда работают только те регионы, которые указаны в документации:&nbsp;https://yandex.ru/dev/webmaster/doc/dg/reference/host-query-analytics.html#request-format__region-ids.<br />
	Сменить регион можно путем исправления значения в двух одинаковых строчках кода:<br />
	&quot;region_ids&quot;: [*****]</li>
	<li>Замечено, что логика работы API меняется буквально каждый день (не зря она в бете). Поэтому работоспособность может нарушаться тем сильнее, чем больше времени пройдет с момента публикования данного скрипта.&nbsp;</li>
</ol>
