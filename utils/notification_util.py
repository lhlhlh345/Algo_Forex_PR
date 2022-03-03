import smtplib
from email.message import EmailMessage
from etl_init import ALERT_CONFIG

from twilio.rest import Client
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



def SMS_alert(message):

    client = Client(ALERT_CONFIG["SMS_account_sid"], ALERT_CONFIG["SMS_auth_token"])
    message = client.messages.create(body=message,
                                     from_=ALERT_CONFIG["SMS_source"],
                                     to=ALERT_CONFIG["SMS_destination"])
    print(message.sid)
    sent_msg = client.messages(message.sid).fetch()
    print(f"message sid: {message.sid}, contents: {sent_msg.body}")

    return None