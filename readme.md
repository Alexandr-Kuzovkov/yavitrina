##Scrapy
[Scrapy Documentation](https://scrapy.readthedocs.io/en/latest/)

`Scrapy is an application framework for crawling web sites and extracting structured data which can be used
for a wide range of useful applications, like data mining, information processing or historical archival
Read more: https://scrapy.org`

##Scrapyd
[Scarapyd Documentation](https://scrapyd.readthedocs.io/en/stable/)

`Scrapyd is an application for deploying and running Scrapy spiders. It enables you to deploy (upload) your
projects and control their spiders using a JSON API.
Read more: http://scrapyd.readthedocs.io/en/stable/`

##Manual
 
#### I. Get project and install docker: 
create `ubuntu` user and login from it
Clone repository and build image:
```bash
git clone https://github.com/kuzovkov/yavitrina.git scrapy-yavitrina
cd scrapy-yavitrina
```

Install docker: [inslall docker documentation](https://docs.docker.com/install/linux/docker-ce/ubuntu/)

OR just run script after clone project
```bash
./install-docker.sh
```

#### II. Build project

```bash
sudo docker-compose build
```

#### III. Configure project

Create config file `/home/ubuntu/vitrina.config.ini`
with content like this:

```ini
[DATABASE]
DB_USER=vitrina
DB_HOST=db
DB_PORT=5432
DB_NAME=vitrina
DB_PASS=P@ssw0rd

[SCRAPESTACK]
ACCESS_KEY=fbb9dd0686xxxxxxxxxxxxxxxxxxxxxx

[MYSQL]
DB_USER=vitrina
DB_HOST=mysql
DB_PORT=3306
DB_NAME=vitrina
DB_PASS=xxxxxxxxx

[SETTINGS]
SPLASH_URL=http://example.com:8050
LOG_LEVEL=INFO

```
 
#### IV. Run project:
    
```bash
sudo docker-compose up -d
sudo docker-compose ps # check that all services are running
```
Go to dashboard, [http://localhost:9000](http://localhost:9000)
Create project with name, example `yavitrina`

#### V. Deploy
On your local developer machine: 
```bash
sudo docker-compose exec scrapy bash
cd /scrapy/yavitrina/
scrapyd-deploy --build-egg output.egg
```
File `output.egg` should appear in `scrapy-yavitrina/yavitrina` folder.
File `output.egg` you should use in Deploy form in dashboard.

#### VI. Run spider

In dashboard press button `Run`, choose spider `vitrina` and press button `Create`
You can see logs of spider in dashboard.

#### VII. Develop and debug
```bash
sudo docker-compose exec scrapy bash
cd /scrapy/yavitrina/
scrapy crawl vitrina # run scraper in console
```

###Database migration tools
------------------------------
[alembic](https://alembic.sqlalchemy.org/en/latest/tutorial.html)

[alembic.op](https://alembic.sqlalchemy.org/en/latest/ops.html)


Install:
```bash
cd /scrapy/yavitrina
mkdir alembic
alembic init alembic
#edit alembic.ini
nano alembic.ini
# edit line 
# sqlalchemy.url = postgresql://dbuser:dbpass/dbname
# ex.:
# sqlalchemy.url = postgresql://vitrina:xxxxxxx/vitrina
```

Create migration:
```bash
cd /scrapy/yavitrina
alembic revision -m "add category column"
chmod -R a+w alembic/versions/
```

Migration file looks like this:

```python

# revision identifiers, used by Alembic.
revision = '1975ea83b712'
down_revision = None
branch_labels = None

from alembic import op
import sqlalchemy as sa

def upgrade():
    pass

def downgrade():
    pass
```

You should update it with what you need:

```python

def upgrade():
    op.create_table(
        'account',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String(50), nullable=False),
        sa.Column('description', sa.Unicode(200)),
    )

def downgrade():
    op.drop_table('account')
```

Run migration:
```bash
alembic upgrade head
```

------------------------------







