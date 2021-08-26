The script for monitoring the cluster of databases

I. Requirements

OS Ubuntu 14.04+
Installed docker, docker-compose


II. Install

git clone <repo_url> failover
cd failover
sudo docker-compose build

III. Run as console command:

sudo docker-compose run failover /app/run

Description:
Script will check that master are alive and defined minimal
number of slaves are alive. If master was died, script will run
command to promote to master on one of slaves and run commands
to bind new master on rest slaves, also notification message
will be sent to email and slack channel.
---------------------------------------------------------------------------------------------
sudo docker-compose run failover /app/run --bind-slave=<SLAVE-IP-ADDRESS>
sudo docker-compose run failover /app/run --bind-slave=<SLAVE-IP-ADDRESS> --master=<MASTER-IP-ADDRESS>
sudo docker-compose run failover /app/run -b <SLAVE-IP-ADDRESS>
sudo docker-compose run failover /app/run -b <SLAVE-IP-ADDRESS> -m <MASTER-IP-ADDRESS>

Description:
Script will only run commands on host <SLAVE-IP-ADDRESS> (stop server, pg_rewind, edit config and
run server again) to bind it to host <MASTER-IP-ADDRESS> if it's passed. If  <MASTER-IP-ADDRESS>
is not passed script will try define <MASTER-IP-ADDRESS> automaticly.

---------------------------------------------------------------------------------------------
sudo docker-compose run failover /app/run --promote-slave=<MASTER-IP-ADDRESS>
sudo docker-compose run failover /app/run -p <MASTER-IP-ADDRESS>

Description:
Script will only run command on host <MASTER-IP-ADDRESS> to promote it to master
---------------------------------------------------------------------------------------------

sudo docker-compose run failover /app/run --basebackup=<SLAVE-IP-ADDRESS>
sudo docker-compose run failover /app/run --basebackup=<SLAVE-IP-ADDRESS> --master=<MASTER-IP-ADDRESS>
sudo docker-compose run failover /app/run --basebackup=<SLAVE-IP-ADDRESS> -m <MASTER-IP-ADDRESS>

Description:
The same that --bind-slave options, exception all data on host <SLAVE-IP-ADDRESS> will be
removed and fetching from scratch using command pg_basebackup.


IV. Run as web application:

sudo docker-compose up -d

Web application will be started on port defined in config.ini file, parameter web.port
"admin/admin" login/password are default.

On page "Nodes" you can see which node is master and are slaves or died.

On page "Stats" you cat see result of query "SELECT * from pg_stat_replication" on master node
to check that all slaves are works as replicas correctly

On page "Log" you can see log file. This view will update every 2 second

On page "Config" you can see and edit config file of this application.

On page "Help" you can read this manual.



