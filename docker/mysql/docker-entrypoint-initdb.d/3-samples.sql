/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

;

--
-- Dumping data for table `category`
--

LOCK TABLES `category` WRITE;
/*!40000 ALTER TABLE `category` DISABLE KEYS */;
INSERT INTO `category` (id, name, parent_id, description, image_path) VALUES (1,'Одежда и обувь',NULL,NULL,'D:\\images'),(2,'Женская обувь',1,'Женская обувь является важным элементом практически любого образа. Несомненно, она способна преобразить любые ножки, передав особенности вкуса и уникальный стиль своей обладательницы. В настоящее время довольно сложно представить свою жизнь без хорошей пары обуви. Брендовый товар представлен во многих магазинах. Однако предлагать наиболее выгодный вариант, к примеру, женских ботинок стараются не все продавцы. Обувь открывает поистине большие возможности применения. На самом деле, не существует единой классификации. Поэтому посмотрим на самые распространенные виды: по закрытости, сезонности и назначению.',''),(3,'Женская домашняя обувь',2,'Качественная обувь домашняя для женщин не роскошь, а настоящая необходимость. Она производится не только для эстетики и создания уюта, у нее есть ряд полезных функций.',''),(4,'Чехлы грязезащитные для женской обуви',NULL,NULL,'D:\\images');
/*!40000 ALTER TABLE `category` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping data for table `category_has_settings`
--

LOCK TABLES `category_has_settings` WRITE;
/*!40000 ALTER TABLE `category_has_settings` DISABLE KEYS */;
INSERT INTO `category_has_settings` (id,settings_id,category_id) VALUES (1,1,2);
/*!40000 ALTER TABLE `category_has_settings` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Dumping data for table `category_search`
--

LOCK TABLES `category_search` WRITE;
/*!40000 ALTER TABLE `category_search` DISABLE KEYS */;
INSERT INTO `category_search` (id,child_id,category_id) VALUES (1,4,2);
/*!40000 ALTER TABLE `category_search` ENABLE KEYS */;
UNLOCK TABLES;



--
-- Dumping data for table `dynamic_price`
--

LOCK TABLES `dynamic_price` WRITE;
/*!40000 ALTER TABLE `dynamic_price` DISABLE KEYS */;
INSERT INTO `dynamic_price` (id,price,date,product_id) VALUES (1,1600.00,'2020-12-01',1),(2,3600.00,'2021-01-01',1),(3,2200.00,'2021-02-01',1),(4,3600.00,'2021-03-01',1),(5,3600.00,'2021-04-01',1),(6,3500.00,'2021-06-01',1);
/*!40000 ALTER TABLE `dynamic_price` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Dumping data for table `new_category`
--

LOCK TABLES `new_category` WRITE;
/*!40000 ALTER TABLE `new_category` DISABLE KEYS */;
INSERT INTO `new_category` (id,category_id,new_category_id) VALUES (1,2,3);
/*!40000 ALTER TABLE `new_category` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Dumping data for table `product`
--

LOCK TABLES `product` WRITE;
/*!40000 ALTER TABLE `product` DISABLE KEYS */;
INSERT INTO `product` (id,title,description,price,is_in_stock,url,url_review,rating,count_review,product_id) VALUES (1,'Сандалии Crocs','Стиль: спортивный, узор: отсутствует, застежка: отсутствует',2510.00,1,'https://market.yandex.ru/search?text=%D0%A1%D0%B0%D0%BD%D0%B4%D0%B0%D0%BB%D0%B8%D0%B8%20Crocs&clid=2336651&distr_type=7&pp=900&vid=7920671p768609049&mclid=1003&cpa=0&onstock=0&local-offers-first=0','https://market.yandex.ru/product--sandalii-crocs-literide-stretch-sandal/768609049/reviews?pp=900&clid=2336651&mclid=1002&distr_type=7',5,3,1),(2,'Сандалии Rieker , размер 38 , белый','Модель: сандалеты, стиль: повседневный, узор: отсутствует, материал верха: натуральная кожа, застежка: липучки',1975.00,1,'https://market.yandex.ru/search?text=%D0%A1%D0%B0%D0%BD%D0%B4%D0%B0%D0%BB%D0%B8%D0%B8+Rieker+%2C+%D1%80%D0%B0%D0%B7%D0%BC%D0%B5%D1%80+38+%2C+%D0%B1%D0%B5%D0%BB%D1%8B%D0%B9&pp=900&mclid=1003&distr_type=7&clid=2336651&vid=7920671p958777578&contentRegion=213','https://market.yandex.ru/product--sandalii-rieker/958777578/reviews?pp=900&clid=2336651&mclid=1002&distr_type=7',5,1,2),(3,'Босоножки Rieker V6273-62','Женские босоножки от знаменитого бренда Швейцарии Rieker. Верх этих босоножек выполнен из натуральной кожи в сером цвете, а внутренняя подкладка на искусственной коже. Отлично подойдут на лето и станут неповторимым аксессуаром к Вашему образу',2625.00,1,'https://market.yandex.ru/search?text=%D0%91%D0%BE%D1%81%D0%BE%D0%BD%D0%BE%D0%B6%D0%BA%D0%B8+Rieker+V6273-62&pp=900&mclid=1003&distr_type=7&clid=2336651&vid=7814990p958723394&contentRegion=213','https://market.yandex.ru/product--bosonozhki-rieker/958723394/reviews?pp=900&clid=2336651&mclid=1002&distr_type=7',5,3,3),(4,'La\'dor шампунь Damaged Protector Acid','Профессиональный шампунь с аргановым маслом и кислотным уровнем pH 4.5 рекомендован для сухих, ослабленных и повреждённых волос. Подходит для устранения и предотвращения появления перхоти. Он очищает волосы и способствует восстановлению волосяных чешуек. Практически не пенится, но хорошо распутывает и разглаживает волосы. Сохраняет стойкость цвета и рекомендован для ежедневного применения для окрашенных волос',1961.00,1,'https://market.yandex.ru/search?text=La%27dor+%D1%88%D0%B0%D0%BC%D0%BF%D1%83%D0%BD%D1%8C+Damaged+Protector+Acid&pp=900&mclid=1003&distr_type=7&clid=2336651&vid=91183p259937113&contentRegion=213','https://market.yandex.ru/product--la-dor-shampun-damaged-protector-acid-dlia-sukhikh-i-povrezhdennykh-volos/259937113/reviews?clid=2336651&distr_type=7&pp=900&mclid=1002&cpa=1',5,180,4);
/*!40000 ALTER TABLE `product` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping data for table `product_category`
--

LOCK TABLES `product_category` WRITE;
/*!40000 ALTER TABLE `product_category` DISABLE KEYS */;
INSERT INTO `product_category`  (id,product_id,category_id) VALUES (1,1,2),(2,2,2),(3,3,2);
/*!40000 ALTER TABLE `product_category` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping data for table `product_color`
--

LOCK TABLES `product_color` WRITE;
/*!40000 ALTER TABLE `product_color` DISABLE KEYS */;
INSERT INTO `product_color` (id,hex,product_id) VALUES (1,'#ffffff',1),(2,'#fc0fc0',1),(3,'#808080',1),(4,'#000000',1);
/*!40000 ALTER TABLE `product_color` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping data for table `product_image`
--

LOCK TABLES `product_image` WRITE;
/*!40000 ALTER TABLE `product_image` DISABLE KEYS */;
INSERT INTO `product_image` (id,path,type,product_id) VALUES (1,'D:\\images','main',1),(2,'D:\\images','child',1);
/*!40000 ALTER TABLE `product_image` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Dumping data for table `product_settings`
--

LOCK TABLES `product_settings` WRITE;
/*!40000 ALTER TABLE `product_settings` DISABLE KEYS */;
INSERT INTO `product_settings` (id,product_id,settings_id,settings_value_id) VALUES (1,1,1,2),(2,4,3,NULL);
/*!40000 ALTER TABLE `product_settings` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping data for table `search_product`
--

LOCK TABLES `search_product` WRITE;
/*!40000 ALTER TABLE `search_product` DISABLE KEYS */;
INSERT INTO `search_product` (id,product_id,child_id) VALUES (1,1,2);
/*!40000 ALTER TABLE `search_product` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping data for table `settings`
--

LOCK TABLES `settings` WRITE;
/*!40000 ALTER TABLE `settings` DISABLE KEYS */;
INSERT INTO `settings`  (id,name) VALUES (1,'застежка'),(2,'материал подкладки'),(3,'pH');
/*!40000 ALTER TABLE `settings` ENABLE KEYS */;
UNLOCK TABLES;


--
-- Dumping data for table `settings_value`
--

LOCK TABLES `settings_value` WRITE;
/*!40000 ALTER TABLE `settings_value` DISABLE KEYS */;
INSERT INTO `settings_value`  (id,settings_id,value) VALUES (1,1,'шнуровка'),(2,1,'отсутсвует'),(3,1,'пряжка'),(4,1,'ремешок'),(5,1,'молния'),(6,2,'текстиль'),(7,2,'натуральная кожа'),(8,2,'исскуственная кожа'),(9,2,'натуральный мех'),(10,2,'натуральная шерсть');
/*!40000 ALTER TABLE `settings_value` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping data for table `tag`
--

LOCK TABLES `tag` WRITE;
/*!40000 ALTER TABLE `tag` DISABLE KEYS */;
INSERT INTO `tag` (id,name,category_id) VALUES (1,'Черная',2),(2,'Рикер',2),(3,'Отико',2),(4,'Ортомода',2),(5,'Немецкая',2),(6,'На широкую ногу ',2),(7,'На танкетке',2),(8,'На подошве ',2),(9,'На платформе',2);
/*!40000 ALTER TABLE `tag` ENABLE KEYS */;
UNLOCK TABLES;


