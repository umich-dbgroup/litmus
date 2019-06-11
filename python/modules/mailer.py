__all__ = ['Mailer']

from email.mime.text import MIMEText

from configparser import ConfigParser
import smtplib

class Mailer(object):
    def __init__(self):
        config = ConfigParser.RawConfigParser(allow_no_value=True)
        config.read('../config.ini')

        self.login = config.get('mailer', 'user')
        self.password = config.get('mailer', 'password')
        self.host = config.get('mailer', 'host')
        self.port = config.get('mailer', 'port')

    def send(self, to_email, subject = 'Done', message = 'Execution finished.'):
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = self.login
        msg['To'] = to_email

        s = smtplib.SMTP(self.host, self.port)
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(self.login, self.password)
        s.sendmail(self.login, [to_email], msg.as_string())
        s.quit()
