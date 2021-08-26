##Scrapy
[Documentation](https://scrapy.readthedocs.io/en/latest/)

`Scrapy is an application framework for crawling web sites and extracting structured data which can be used
for a wide range of useful applications, like data mining, information processing or historical archival
Read more: https://scrapy.org`

##Scrapyd
[Documentation](https://scrapyd.readthedocs.io/en/stable/)

`Scrapyd is an application for deploying and running Scrapy spiders. It enables you to deploy (upload) your
projects and control their spiders using a JSON API.
Read more: http://scrapyd.readthedocs.io/en/stable/`

##Manual
 
###I. Install

Tested on `Ubuntu 14.04`, `16.04`

####I. Install packages

```bash
sudo apt-get update && \
apt-get install -y software-properties-common apt-utils python-software-properties && \
apt-get install -y python-dev libxml2-dev python-pip zlib1g-dev libffi-dev libssl-dev libxslt1-dev \
net-tools
```

####II. Create file requirements.txt 

```bash
echo "
psycopg2
scrapy
scrapyd
scrapy-splash
Werkzeug==0.12.1
six==1.10.0
Jinja2==2.9.6
aniso8601==1.2.0
APScheduler==3.3.1
click==6.7
Flask==0.12.1
Flask-BasicAuth==0.2.0
Flask-RESTful==0.3.5
flask-restful-swagger==0.19
Flask-SQLAlchemy==2.2
itsdangerous==0.24
Jinja2==2.9.6
MarkupSafe==1.0
PyMySQL==0.7.11
python-dateutil==2.6.0
pytz==2017.2
requests==2.13.0
SQLAlchemy==1.1.9
tzlocal==1.3
transliterate
Werkzeug==0.12.1
spiderkeeper
" > ~/requirements.txt
```

####III. Install requirements

```bash
export LC_ALL="en_US.UTF-8"
export LC_CTYPE="en_US.UTF-8"
sudo dpkg-reconfigure locales
sudo pip install --no-cache-dir -r requirements.txt
```

####IV. Install docker: 
Install docker: [link](https://docs.docker.com/install/linux/docker-ce/ubuntu/)

####V. Install `Splash`

[Documentation](https://splash.readthedocs.io/en/stable)

```
Splash is a javascript rendering service. It’s a lightweight web browser with an HTTP API, 
implemented in Python 3 using Twisted and QT5. The (twisted) QT reactor is used to make 
the service fully asynchronous allowing to take advantage of webkit concurrency via QT main loop. 
Some of Splash features:

    1. process multiple webpages in parallel;
    2. get HTML results and/or take screenshots;
    3. turn OFF images or use Adblock Plus rules to make rendering faster;
    4. execute custom JavaScript in page context;
    5. write Lua browsing scripts;
    6. develop Splash Lua scripts in Splash-Jupyter Notebooks.
    7. get detailed rendering info in HAR format.
```

```bash
sudo docker pull scrapinghub/splash
```

####VI. Create service configs in `/etc/systemd/system/`

```bash
sudo echo "
[Unit]
Description=Scrapy service
After=network.target
[Service]
User=ubuntu
PIDFile=/home/ubuntu/scrapyd.pid
WorkingDirectory=/home/ubuntu
ExecStart=/usr/local/bin/scrapyd
[Install]
WantedBy=multi-user.target
" > /etc/systemd/system/scrapyd.service
```
--
```bash
sudo echo "
[Unit]
Description=SpiderKeeper service
After=network.target
[Service]
User=ubuntu
PIDFile=/home/ubuntu/spiderkeeper.pid
WorkingDirectory=/home/ubuntu
ExecStart=/usr/local/bin/spiderkeeper --server=http://localhost:6800 --database-url=sqlite:////home/ubuntu/SpiderKeeper.db --username=admin --password=admin
[Install]
WantedBy=multi-user.target
" > /etc/systemd/system/spiderkeeper.service
```

######Note

In the string: 

`ExecStart=/usr/local/bin/spiderkeeper --server=http://localhost:6800 --database-url=sqlite:////home/ubuntu/SpiderKeeper.db --username=admin --password=admin`
you can define password for SpiderKeeper application ( --username=admin --password=admin).

--
```bash
sudo echo "
[Unit]
Description = Splash service
After = network.target

[Service]
User = ubuntu
PIDFile = /home/ubuntu/splashd.pid
WorkingDirectory = /home/ubuntu
ExecStart = /usr/local/bin/splashd

[Install]
WantedBy = multi-user.target
" > /etc/systemd/system/splashd.service
```
    
####VII. Create file and add execute permission `/usr/local/bin/splashd`

```bash
sudo echo "#!/bin/sh
sudo docker run -p 5023:5023 -p 8050:8050 -p 8051:8051 scrapinghub/splash --max-timeout 3600 --disable-lua-sandbox
" >  /usr/local/bin/splashd
sudo chmod a+x /usr/local/bin/splashd
```

####VIII. Run services and check statuses:

```bash
sudo systemctl start splashd
sudo systemctl start spiderkeeper
sudo systemctl start scrapyd

sudo systemctl status splashd
sudo systemctl status spiderkeeper
sudo systemctl status scrapyd
```

####IX. Install and configure `nginx`

```bash
sudo apt-get install nginx
```
 
--
```bash
sudo echo " 
server {
listen 80;
#listen 443 ssl;
#server_name feeds.xtramile.io;
root /boardfeeds;
#ssl_certificate www.example.com.crt;
#ssl_certificate_key www.example.com.key;
index index.html;
autoindex on;

location / {
if ($request_method = POST) {
rewrite ^/(.*)$ /post_redirect/$1 last;
}
try_files $uri $uri/ =404;
}

location ~ ^/post_redirect/(.*)$ {
internal;
proxy_set_header Host $http_host;
set $proxy_url http://127.0.0.1:5000/$1;
if ($args) {
set $proxy_url http://127.0.0.1:5000/$1?$args;
}
proxy_pass $proxy_url;
}


location /console/ {
proxy_pass http://127.0.0.1:5000/;
}

location /static/css/ {
proxy_pass http://127.0.0.1:5000/static/css/;
}

location /static/js/ {
proxy_pass http://127.0.0.1:5000/static/js/;
}

location /static/fonts/ {
proxy_pass http://127.0.0.1:5000/static/fonts/;
}


location /api/_static/css/ {
proxy_pass http://127.0.0.1:5000/api/_static/css/;
}

location /api/_static/lib/ {
proxy_pass http://127.0.0.1:5000/api/_static/lib/;
}

location /api/_static/images/ {
proxy_pass http://127.0.0.1:5000/api/_static/images/;
}



location /project {
return 307 $scheme://$http_host/console$request_uri;
}

#location /api {
# return 301 $scheme://$http_host/console$request_uri;
#}

}
# proxy_read_timeout 1200;
# proxy_connect_timeout 240;
# client_max_body_size 0;
" > /etc/nginx/sites-available/feeds.xtramile.io
```
--
```bash
sudo ln -s /etc/nginx/sites-available/feeds.xtramile.io /etc/nginx/sites-enabled/feeds.xtramile.io
sudo rm /etc/nginx/sites-enabled/default
sudo systemctl restart nginx
```

####X. Copy to `~` files `geobase.sqlite`, `geoname.sqlite`, `db.conf`

create folder `~/utils/autorun`

copy to `~/utils/autorun/autorun.py` (https://bitbucket.org/xtramile/jobscrapers/src/f507fde247616bd64fca747d30ae77667224622f/jobscrapers/utils/?at=master)    

create `~/utils/autorun/autorun.conf`:

--

```bash
echo "
[database]
dbname = xtramile_prod
dbhost = tools.xtramile.tech
dbport = 5432
dbuser = postgres
dbpass = aDb91-UxT*1@l%tnopZ

[logger]
logfile = /home/ubuntu/utils/autorun/autorun.log
logger_level = 5
status_logfile = /home/ubuntu/utils/autorun/status.log
status_logger_level = 5

[status]
k = 0.96
status = 5
positive_balance_limit = 2

[scrapyd]
server = http://127.0.0.1:6800/
api_listspiders = listspiders.json?project=jobscrapers
api_listjobs = listjobs.json?project=jobscrapers
max_run_jobs = 3

[spiderkeeper]
server = http://127.0.0.1:5000/
#api_key = 3efvbgtr4
username = admin
password = admin
api_projects = api/projects
api_spiders = api/projects/%s/spiders
api_spider_run = api/projects/%s/spiders/%s
api_spider_detail = api/projects/%s/spiders/%s
api_job_list = api/projects/%s/jobexecs
" > ~/utils/autorun/autorun.conf
```

####XI. Go to `http://<a-scrapy-ip>/console`

(admin/admin)

Create project and deploy them (jobscrapers, jobimporters, feedgenerator, monitor)

####XII. Put tasks to cron
```bash
crontab -e
```
--

    */20 * * * * /home/ubuntu/utils/autorun/autorun.py -c /home/ubuntu/utils/autorun/autorun.conf
    10 */4 * * * /home/ubuntu/utils/autorun/autorun.py -c /home/ubuntu/utils/autorun/autorun.conf --run-spider=jobimporters,jobleads --args='employer_id=101'
    20 */4 * * * /home/ubuntu/utils/autorun/autorun.py -c /home/ubuntu/utils/autorun/autorun.conf --run-spider=jobimporters,jobmonitor --args='employer_id=104'
    10 1 * * * /home/ubuntu/utils/autorun/autorun.py -c /home/ubuntu/utils/autorun/autorun.conf --run-spider=jobimporters,dejobmonitor --args='employer_id=119'
    30 */4 * * * /home/ubuntu/utils/autorun/autorun.py -c /home/ubuntu/utils/autorun/autorun.conf --run-spider=jobimporters,tata --args='employer_id=134'
    40 */4 * * * /home/ubuntu/utils/autorun/autorun.py -c /home/ubuntu/utils/autorun/autorun.conf --run-spider=jobimporters,general --args='employer_id=143'
    #50 */4 * * * /home/ubuntu/utils/autorun/autorun.py -c /home/ubuntu/utils/autorun/autorun.conf --run-spider=gcd_apec_feedgenerator,gcd --args='board_id=168'
    
    0 */4 * * * /home/ubuntu/utils/autorun/autorun.py -c /home/ubuntu/utils/autorun/autorun.conf --run-spider=feedgenerator,jobintree2 --args='board_id=9,azure=true'
    2 */4 * * * /home/ubuntu/utils/autorun/autorun.py -c /home/ubuntu/utils/autorun/autorun.conf --run-spider=feedgenerator,jobintree2 --args='board_id=12,azure=true'
    4 */4 * * * /home/ubuntu/utils/autorun/autorun.py -c /home/ubuntu/utils/autorun/autorun.conf --run-spider=feedgenerator,jobintree2 --args='board_id=15,azure=true'
    
####XIII. Other

Clear Next Jobs list from feedgenerator project as example (project id =1)

```bash
ssh a-scrapy
sudo apt-get install sqlite3 # if not yet installed
sqlite3 SpiderKeeper.db "delete from sk_job_execution where running_status=0 and project_id=1;"
```
The same in docker:
```bash
sudo docker-compose exec spiderkeeper bash
apt-get install sqlite3
sqlite3 SpiderKeeper.db "delete from sk_job_execution where running_status=0 and project_id=1;"
```

###XIV. Install in Docker

create `ubuntu` user and login from it

Clone repository and build image:

```bash
git pull https://bitbucket.org/xtramile/jobscrapers scrapy
cd scrapy
sudo docker-compose build
```
    
copy this files to `/home/ubuntu`:
    
    db.conf
    autorun.conf
    geoname.sqlite
    geobase.sqlite


autorun.conf:

    [database]
    dbname = xtramile_prod
    dbhost = pg.xtramile.io
    dbport = 5400
    dbuser = xt_main
    dbpass = zxcjJAQ98><xNb3610292UpklLQghj*&!@#$%^&azxcvyrtRR
    
    [logger]
    logfile = /home/ubuntu/utils/autorun/autorun.log
    logger_level = 5
    status_logfile = /home/ubuntu/utils/autorun/status.log
    status_logger_level = 5
    
    [status]
    k = 0.96
    status = 5
    positive_balance_limit = 2
    
    [scrapyd]
    server = http://127.0.0.1:6800/
    api_listspiders = listspiders.json?project=feedgenerator
    api_listjobs = listjobs.json?project=feedgenerator
    max_run_jobs = 4
    
    [spiderkeeper]
    server = http://spiderkeeper:5000/
    #api_key = 3efvbgtr4
    username = admin
    password = admin
    api_projects = api/projects
    api_spiders = api/projects/%s/spiders
    api_spider_run = api/projects/%s/spiders/%s
    api_spider_detail = api/projects/%s/spiders/%s
    api_job_list = api/projects/%s/jobexecs


db.conf:

    [prod]
    dbname=xtramile_prod
    dbhost=pg.xtramile.io
    dbport=5400
    dbuser=xt_main
    dbpass=password
    
    [coreapi_prod]
    dbname=xtramile
    dbhost=pg.xtramile.io
    dbport=5400
    dbuser=xt_main
    dbpass=password
    
    [slave_prod]
    dbname=xtramile_prod
    dbhost=pg.xtramile.io
    dbport=5401
    dbuser=xt_main
    dbpass=password


Create folder for feeds:

```bash
mkdir /boardfeeds
chmod -R 777 /boardfeeds
sudo docker-compose up -d
```

Add to cron:

```bash
   */20 * * * *    sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy  /home/root/autorun.py -c /home/root/autorun.conf
    #5 * * * *     sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy /home/root/autorun.py -c /home/root/autorun.conf --run-spider=feedgenerator,general4 --args='azure=true,port=6800'
    10 */4 * * *   sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy /home/root/autorun.py -c /home/root/autorun.conf --run-spider=jobimporters,jobleads --args='employer_id=101'
    20 */4 * * *   sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy /home/root/autorun.py -c /home/root/autorun.conf --run-spider=jobimporters,jobmonitor --args='employer_id=104'
    
    30 */4 * * *   sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy /home/root/autorun.py -c /home/root/autorun.conf --run-spider=jobimporters,tata --args='employer_id=134'
    40 */4 * * *   sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy /home/root/autorun.py -c /home/root/autorun.conf --run-spider=jobimporters,general --args='employer_id=143'
    45 */4 * * *  sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy /home/root/autorun.py -c /home/root/autorun.conf --run-spider=gcd_apec_feedgenerator,gjp --args='board_id=168,port=6800'
    35 */4 * * *  sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy /home/root/autorun.py -c /home/root/autorun.conf --run-spider=gcd_apec_feedgenerator,apec --args='board_id=169,port=6800'
    0 */4 * * *   sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy /home/root/autorun.py -c /home/root/autorun.conf --run-spider=feedgenerator,jobintree2 --args='board_id=9,azure=true,port=6800'
    14 */4 * * *  sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy  /home/root/autorun.py -c /home/root/autorun.conf --run-spider=feedgenerator,jobintree2 --args='board_id=12,azure=true,port=6800'
    24 */4 * * *  sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy /home/root/autorun.py -c /home/root/autorun.conf --run-spider=feedgenerator,jobintree2 --args='board_id=15,azure=true,port=6800'
```

####XV. Deploy

You can do this manually, but the easiest way is to use the `scrapyd-deploy`  tool provided by `scrapyd-client`.

Install  `scrapyd-client`:

```bash
sudo pip install scrapyd-client
```

Build project package:

```bash
cd <project forder>
scrapyd-deploy --build-egg output.egg
```
Deploy:

Open SpiderKeeper: `http://<a-scrapy-ip>/console`
Choose project, go to menu `Deploy`, choose builded .egg file and push `"Submit"` button


####XVI. Develop and debug

Run spiders on local:

Enter to local project folder. 

Examples:

Project `jobscrapers`

```bash
cd jobscrapers
scrapy crawl <spider_name> -a <parameter_name>=<parameter_value>
scrapy crawl pwcrecruit
```

Project `feedgenerator`

```bash
cd feedgenerator
scrapy crawl general4 -a azure=true -a force=true
```
    
Project `jobimporters`

```bash
cd jobimporters
scrapy crawl general -a employer_id=134
```

###Control jobimport

#####Checking that all works properly

 * Go to `SpiderKeeper` page [http://40.118.41.25/console/](http://40.118.41.25/console/)
 * Select in menu `Projects`  project `jobimporters`: here you can see `Completed Jobs` list.
 * Click on link `Log` and scroll down
 * See `Dumping Scrapy stats`, it looks like this:
```
2019-03-19 04:41:50 [scrapy.statscollectors] INFO: Dumping Scrapy stats:
{'downloader/request_bytes': 510,
'downloader/request_count': 2,
'downloader/request_method_count/GET': 2,
'downloader/response_bytes': 5215845,
'downloader/response_count': 2,
'downloader/response_status_count/200': 1,
'downloader/response_status_count/403': 1,
'finish_reason': 'finished',
'finish_time': datetime.datetime(2019, 3, 19, 4, 40, 53, 119014),
'item_scraped_count': 2959,
'log_count/INFO': 15,
'memusage/max': 134832128,
'memusage/startup': 134832128,
'response_received_count': 2,
'robotstxt/request_count': 1,
'robotstxt/response_count': 1,
'robotstxt/response_status_count/403': 1,
'scheduler/dequeued': 1,
'scheduler/dequeued/memory': 1,
'scheduler/enqueued': 1,
'scheduler/enqueued/memory': 1,
'start_time': datetime.datetime(2019, 3, 19, 4, 40, 12, 615074)}
```

If you should't see lines with `log_count/ERROR`. If not, I need search in the log what happens and solve problem.

#####When we get notification to email

Sometimes we can get email with subject `JobLeads jobimport notify` and content like this:

```
Job external_unique_id="e5dcf3661d378659036343d3cbf005b7a_101" category="6" and country="Spain" not imported because job Group not found
2019-03-19 12:13:27: "jobleads" Unknown Job Group was found!
```

This means that appropriate job group for employer `JobLeads`, job's category and country `Spain` does not exists. To solve this you need 
login to recruitep application to  `JobLeads` account, go to `X-Jobs` -> `Groups` menu and create job group with name like this:
<category name> - <countru ISO-2 code>. For instance: `HR & Recruitment Jobs-ES`

Category code see here:

```
1 => 'Accounting & Finance Jobs',
2 => 'IT Jobs',
3 => 'Sales Jobs',
4 => 'Customer Services Jobs',
5 => 'Engineering Jobs',
6 => 'HR & Recruitment Jobs',
7 => 'Healthcare & Nursing Jobs',
8 => 'Hospitality & Catering Jobs',
9 => 'PR, Advertising & Marketing Jobs',
10 => 'Logistics & Warehouse Jobs',
11 => 'Teaching, Training & Scientific Jobs',
12 => 'Trade & Construction Jobs',
13 => 'Admin Jobs',
14 => 'Legal Jobs',
15 => 'Culture & Medias',
16 => 'Graduate Jobs',
17 => 'Retail Jobs',
18 => 'Consultancy Jobs',
19 => 'Manufacturing & Craftsmanship Jobs',
20 => 'Agriculture & Environmental Jobs',
21 => 'Social work Jobs',
22 => 'Travel Jobs',
23 => 'Energy, Oil & Gas Jobs',
24 => 'Property Jobs',
25 => 'Charity & Voluntary Jobs',
26 => 'Domestic help & Cleaning Jobs',
27 => 'Installation & Maintenance Jobs',
28 => 'Part time Jobs',
29 => 'Defence jobs',
30 => 'Other/General Jobs',
31 => 'Chartered accountancy',
32 => 'Logistics',
33 => 'Setting/Control',
34 => 'Automation/Robotics',
35 => 'Drawing/Studies',
36 => 'Electrical',
37 => 'Maintenance',
38 => 'Mounting/Assembly'
```

Choose appropriate country in list. 
Id country does not exists, still country field empty.
After group created, execute SQL query in database: 
```sql
SELECT * FROM job_groups WHERE employer_id=101
```

Find row with new job_group added and fill country field ISO-2 country code manually.

#####Start/Stop

You can disable running jobimport by comment appropriate line in crontab

```bash
crontab -e
```

You can run jobimport manually from SpiderKeeper:

  * Go to `SpiderKeeper` page [http://40.118.41.25/console/](http://40.118.41.25/console/)
  * Select in menu `Projects` project `jobimporters`.
  * Click `RunOnce` button, choose spider, enter `employer_id` parameter appropriate employer ID in `Args` field.
  * Click `Create`
  
#####Import from a new source

For import jobs of new employer, this employer must be created in Backoffice. 
Parametr `Feed url` of employer must contain URL of him XML feed. Employer can have many feeds. In this case 
in `Feed url` there are  comma separated list of URLs.
Also Employer Feed Settings must be fill properly when when process employer creating or directly edit database table
`employer_feed-settings`. This need to correct mapping XML fiels to database table `jobs` field.
Field `root` content root tag:
For instance:
XML snippet                         
```xml
<jobs>         
   <job>
        <title>title</title>
        <category>category</category>
        <link>http://job.link</link>
        <desc>Job description</desc>
        <location>
            <country>France</country>
        </location>
        ...
   </job> 
</jobs>
``` 

Field `root` should content `jobs`
Field `job` content job tag: `job`
                           
The rest fields need for mapping tag name to jobs table column name.
For example field `url` should be `link`, field `description` should be `desc`
Field `country` should be `location/country` and so on...

We can use existing scraper or write separate scraper for new employer, if need some
especial logic for import jobs of new employers.

Scraper may be run from local:

```bash
cd jobscrapers/jobimporters
scrapy crawl general -a employer_id=<employer-id>
```

Or you can build `egg` package and deploy it use SpiderKeeper form.

```bash
cd jobscrapers/jobimporters
scrapyd-deploy --build-egg output.egg
```

The same in container:
```bash
sudo docker-compose exec scrapy bash
cd /scrapy/fibois
pip install scrapyd-client #if not installed yet
scrapyd-deploy --build-egg output.egg
```

Select in menu `Projects` -> `jobimporters` project `jobimporters`.
Click `Deploy` in left menu. Push button `Choose File`, choose `output.egg`, and push `Submit`.
Run spider as described above.

###Feed's generation

#####Checking that all works properly

You can check generated XML feeds (size, date) on the URL  [http://40.118.41.25/](http://40.118.41.25/)
The same XML files are pushed to Azure Storage.

 * Go to `SpiderKeeper` page [http://40.118.41.25/console/](http://40.118.41.25/console/)
 * Select in menu `Projects` project `feedgenerator`: here you can see `Completed Jobs` list.
 * Click on link `Log` and scroll down
 * See `Dumping Scrapy stats`, it looks like this:
```
Dumping Scrapy stats:
{'downloader/request_bytes': 208,
'downloader/request_count': 1,
'downloader/request_method_count/GET': 1,
'downloader/response_bytes': 880,
'downloader/response_count': 1,
'downloader/response_status_count/200': 1,
'finish_reason': 'finished',
'finish_time': datetime.datetime(2019, 3, 19, 20, 40, 21, 432135),
'log_count/DEBUG': 1,
'log_count/INFO': 17,
'log_count/WARNING': 2,
'memusage/max': 70479872,
'memusage/startup': 70479872,
'response_received_count': 1,
'scheduler/dequeued': 1,
'scheduler/dequeued/memory': 1,
'scheduler/enqueued': 1,
'scheduler/enqueued/memory': 1,
'start_time': datetime.datetime(2019, 3, 19, 20, 40, 11, 583737)}
```

If you should't see lines with `log_count/ERROR`. If not, I need search in the log what happens and solve problem.

#####Start/stop

You can run feednenerator scraper manually from SpiderKeeper:

  * Go to `SpiderKeeper` page [http://40.118.41.25/console/](http://40.118.41.25/console/)
  * Select in menu `Projects` project `feedgenerator`.
  * Click `RunOnce` button, choose spider `general4`, enter `azure=true,port=6800` parameters in `Args` field.
  * For generation feeds jobboards `capital`, `jobintree`, `vivastreet` choose spider `jobintree2` and enter `board_id=9(12,15),azure=true,port=6800` parameters in `Args` field.
  * Click `Create`

You can disable running jobimport by comment appropriate line in crontab

```bash
crontab -e
```

There are lines for run feedgeneration scripts:
```bash
*/20 * * * *    sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy  /home/root/autorun.py -c /home/root/autorun.conf
0 */4 * * *   sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy /home/root/autorun.py -c /home/root/autorun.conf --run-spider=feedgenerator,jobintree2 --args='board_id=9,azure=true,port=6800'
14 */4 * * *  sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy  /home/root/autorun.py -c /home/root/autorun.conf --run-spider=feedgenerator,jobintree2 --args='board_id=12,azure=true,port=6800'
24 */4 * * *  sudo docker-compose -f /home/ubuntu/scrapy/docker-compose.yml exec scrapy /home/root/autorun.py -c /home/root/autorun.conf --run-spider=feedgenerator,jobintree2 --args='board_id=15,azure=true,port=6800'
```

#####To generate new XML feed for new job board: 
* Create job board in Backoffice.
* Set feed's field names in job board `FEED SETTINGS`. [example](https://backoffice.xtramile.io/#/job-boards/1).
This fields mean in which tag name will be mapped each `jobs` table column.
Set `Update Frequency`. This parameter means how may times feed will be update per day.
* Created job_board should be linked with job group. Go to menu `Job groups`. Select job group you need from list, 
click on green icon `Job boards of job group`. Click `Add new job_board for job group`. Click new job board in pop up list and then `Save` icon.
Click green triangle in circle to enable added job board.
  
####Important files

There are next importamt files in the `/home/ubuntu` on `a-scrapy` machine:
* `db.conf` - database connection parameters;
* `geoname.sqlite`, `geobase.sqlite` - sqlite databases to perform geographical search.
* `autorun.conf` - config file for `autorun.py` script

`autorun.py` is the script, which run feedgenerator's script by schedule, use SpiderKeeper API.


###Troubleshuting


`/usr/local/bin/scrapyd-deploy:23: ScrapyDeprecationWarning: 
Module scrapy.utils.http is deprecated, Please import from w3lib.http instead.`

Solution:
-------------

in scrapy container:
----------------------
```bash
apt-get install nano
nano /usr/local/bin/scrapyd-deploy:

from scrapy.utils.project import inside_project
#from scrapy.utils.http import basic_auth_header
from w3lib.http import basic_auth_header
```
-----------------------------







