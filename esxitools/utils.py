def send_email(gmail_user, gmail_pwd, from_field, recipients, subject, body):
    import smtplib
    if isinstance(recipients, list):
        to_list = list(recipients)
    else:
        to_list = list([recipients])
    # Prepare actual message
    message = u'From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s\r\n' %    \
                (from_field, ', '.join(to_list), subject, body)
    server = smtplib.SMTP(u'smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    server.login(gmail_user, gmail_pwd)
    server.sendmail(from_field, to_list, message)
    server.close()
