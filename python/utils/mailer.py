__all__ = ['Mailer']

from email.mime.text import MIMEText

import smtplib

class Mailer:
    def __init__(self, login = 'cannoliemailer@gmail.com', password = 'cannoli123', host = 'smtp.gmail.com', port = 587):
        self.login = login
        self.password = password
        self.host = host
        self.port = port

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
