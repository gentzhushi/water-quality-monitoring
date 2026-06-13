import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr

from email_template import build_html, build_subject, build_text


def build_email_message(config, subject, text_content, html_content):
    message = EmailMessage()
    message["From"] = formataddr(
        (
            config.smtp_sender_name,
            config.smtp_sender_email,
        )
    )
    message["To"] = ", ".join(config.recipients)
    message["Subject"] = subject
    message.set_content(text_content)
    message.add_alternative(html_content, subtype="html")
    return message


class EmailChannel:
    def __init__(self, config):
        self.config = config

    def send(self, alert):
        subject = build_subject(alert)
        text_content = build_text(alert)
        html_content = build_html(alert)

        if self.config.dry_run:
            print("Notification dry run: email was not sent", flush=True)
            print(f"Subject: {subject}", flush=True)
            print(text_content, flush=True)
            return True

        if not self.config.recipients:
            print("Cannot send email: NOTIFICATION_RECIPIENTS is empty", flush=True)
            return False

        if not self.config.smtp_username:
            print("Cannot send email: SMTP_USERNAME is missing", flush=True)
            return False

        if not self.config.smtp_password:
            print("Cannot send email: SMTP_PASSWORD is missing", flush=True)
            return False

        if not self.config.smtp_sender_email:
            print("Cannot send email: SMTP_SENDER_EMAIL is missing", flush=True)
            return False

        message = build_email_message(
            self.config,
            subject,
            text_content,
            html_content,
        )
        tls_context = ssl.create_default_context()

        try:
            with smtplib.SMTP(
                self.config.smtp_host,
                self.config.smtp_port,
                timeout=10,
            ) as server:
                server.starttls(context=tls_context)
                server.login(self.config.smtp_username, self.config.smtp_password)
                server.send_message(
                    message,
                    from_addr=self.config.smtp_sender_email,
                    to_addrs=self.config.recipients,
                )
        except (OSError, smtplib.SMTPException) as error:
            print(f"SMTP email send failed: {error}", flush=True)
            return False

        print(f"SMTP email sent to {', '.join(self.config.recipients)}", flush=True)
        return True
