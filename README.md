# Catreen
________________________________________
K3DBMS is a ligthweight object database library for Windows Vista+ kernels, C++
Project news
•	09-oct-2010. Released core version 2.0-alpha. See in trunk\unittests
•	11-oct-2010. Released core version 2.0 downloadable unittest.
Features
English
supports such functionality as: * easy of use with native C++ objects; * faster than ORM - no overhead; * thread-safe advanced caching; * transactions (NTFS TxF-based, RC level); * store complex objects as standartized in JSON; * automatic base OLAP structures; * no other libraries used, just JSON (if need); * only 100 Kb codebase; * fully customizable storage and caching engine; * Windows XP backward - compatible (no ACID);
Russian
1.	Простой доступ из языка программирования, удобная работа с единичными объектами.
2.	RENEW Отсутствие необходимости «привязывать» сущности прикладной области к реляционным таблицам, или создания маппингов ORM;
3.	ACID;
4.	Скорость работы выше, чем у ORM;
5.	Прозрачная многопоточность внутри приложения;
6.	Многоуровневое кэширование;
7.	Эффективно хранит данные – на диске файлы данных могут занять меньше места, чем их реальный размер; (A)
8.	RENEW.Работа с множествами связанных объектов. Поддержка объектов сложной структуры согласно JSON, со свойствами переменной длины – массивами и списками;
9.	Компактный размер и простота реализации;
10.	RENEW. Широкие возможности по оптимизации структуры хранения данных, индивидуальные настройки кэширования множеств и индексов.
11.	RENEW. Автопересчет зависимых множеств, элементы OLAP.
12.	Использует передовые технологии Windows Server 2008, Vista и Windows 7:
1.	Эффективное использование памяти;
2.	Масштабируемость по памяти, процессорам, дисковой емкости;
3.	Высококонкурентный многопроцессный доступ к файлам БД;
4.	Возможности NTFS V6.0 - экстенты, сжатие, безопасность, надежность, транзакции;
5.	Volume Shadow Copy безопасно для целостности копии БД;
13.	Перспективы реализации (B) – кластерные системы, распределенные транзакции, сетевой-файловый конкурентный доступ, высокие уровни трансизоляции, БЛОБы, параллелизм дисковых операций;
14.	Свободная лицензия LGPL3 для использования в любых целях.
(A) Законы сохранения никто не отменял, но есть трики. (B) Чем сложнее, те более дальние.

