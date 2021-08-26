import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import urllib
#import urllib.request as urllib2 #for python3
import urllib2 #for python2
import json
import ssl
import os.path
import pkgutil
from string import Template


class Notifier:
    spider = None
    logger = None
    emails = None

    def __init__(self, options, logger):
        self.logger = logger
        self.options = options

    def slack(self, message):
        slack_channel = self.options.get('SLACK_CHANNEL', '#incident')
        slack_web_hook_point = self.options.get('SLACK_WEB_HOOK_POINT', '')
        if hasattr(self.spider, 'env') and self.spider.env == 'dev':
            slack_channel = '-'.join([slack_channel, 'dev'])
        data = urllib.urlencode({'payload': json.dumps(
            {'channel': slack_channel, 'text': message, 'username': 'Xtramile-chat-bot', 'icon_emoji': ':ghost:'})})
        req = urllib2.Request(url=slack_web_hook_point)
        req.add_data(data)
        try:
            response = urllib2.urlopen(req)
        except Exception as ex:
            self.logger.error('Send slack notify fail!')
        else:
            self.logger.info('Send slack notify successfully')

    def email(self, subject, msg, template_path=None, dev=False):
        sender_email = self.options.get('EMAIL_FROM', '')
        receiver_email = self.options.get('NOTIFY_EMAILS', [])
        if dev == True:
            receiver_email = self.options.get('NOTIFY_EMAILS_DEV', [])
        password =  self.options.get('SMTP_PASSWORD', [])
        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = sender_email
        if type(receiver_email) is list:
            message['To'] = ','.join(receiver_email)
        else:
            message["To"] = receiver_email
        port = self.options.get('SMTP_PORT', 465)
        smtp_host = self.options.get('SMTP_SERVER', 'smtp.yandex.ru')
        # Create the plain-text and HTML version of your message
        text = msg
        # Add HTML/plain-text parts to MIMEMultipart message
        part1 = MIMEText(text, "plain")
        message.attach(part1)
        # Turn these into plain/html MIMEText objects
        if template_path is not None:
            html_template = pkgutil.get_data('fibois', template_path)
            if html_template is not None:
                t = Template(html_template)
                html = t.substitute({'message': msg})
                part2 = MIMEText(html, "html")
                message.attach(part2)

        # The email client will try to render the last part first
        # Create secure connection with server and send email
        #context = ssl.create_default_context() for python 3
        try:
            server = smtplib.SMTP_SSL(smtp_host, port)
            server.login(sender_email, password)
            # server.set_debuglevel(1)
            server.sendmail(sender_email, receiver_email, message.as_string())
            server.quit()
        except Exception as ex:
            self.logger.error('ERROR: sending email fail: %s' % ex)
            return False
        else:
            self.logger.info('Sending email sucessfully')
            return True