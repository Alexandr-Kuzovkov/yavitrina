import urllib3
import json
from myconfig import config as config
from mylogger import logger as logger
import urllib
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def slack(message, slack_channel=None):
    if slack_channel is None:
        slack_channel = config.get('notify', 'slack_channel')
    slack_web_hook_point = config.get('notify', 'slack_web_hook_point')
    #data = urllib.parse.urlencode({'payload': json.dumps({'channel': slack_channel, 'text': message, 'username': 'Xtramile-chat-bot', 'icon_emoji': ':ghost:'})})
    http = urllib3.PoolManager()
    encoded_data = 'payload=' + json.dumps({'channel': slack_channel, 'text': message, 'username': 'Xtramile-chat-bot', 'icon_emoji': ':ghost:'})
    #print(encoded_data)
    try:
        res = http.request(method='POST', url=slack_web_hook_point, body=encoded_data, headers={'Content-Type': 'application/x-www-form-urlencoded'})
    except Exception as ex:
        logger.error('Send slack notify fail!')
    else:
        logger.info('Send slack notify successfully')
        logger.info(res.data.decode('utf-8'))


def email(subject, message):
    toaddr = json.loads(config.get('notify', 'notify_emails'))
    fromaddr = config.get('notify', 'email_from')
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    if type(toaddr) is list:
        msg['To'] = ','.join(toaddr)
    else:
        msg['To'] = toaddr
    msg['Subject'] = subject
    body = message
    msg.attach(MIMEText(body, 'plain'))
    try:
        server = smtplib.SMTP(config.get('notify', 'smtp_server'), config.get('notify', 'smtp_port'))
        server.starttls()
        server.login(fromaddr, config.get('notify', 'smtp_password'))
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()
    except Exception as ex:
        logger.error('ERROR: sending email fail: %s' % ex)
        return False
    else:
        logger.info('Sending email sucessfully')
        return True