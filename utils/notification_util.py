import smtplib
from email.message import EmailMessage
from etl_init import ALERT_CONFIG

from etl_init import ALERT_CONFIG

def email_alert(subject, body, to):
    msg = EmailMessage()
    msg.set_content(body)
    msg['subject'] = subject
    msg['to'] = to

    user = ALERT_CONFIG['emailACCT']
    msg["from"] = user
    password = ALERT_CONFIG['emailPWD']

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()
    server.login(user, password)
    server.send_message(msg)

    server.quit()

