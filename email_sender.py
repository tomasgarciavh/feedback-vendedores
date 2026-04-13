import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import config

logger = logging.getLogger(__name__)

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465


def _markdown_to_html(text: str) -> str:
    """
    Convert a simple markdown-like text to HTML.
    Handles: ### headers, **bold**, newlines, --- separators.
    """
    import re

    lines = text.split("\n")
    html_lines = []
    for line in lines:
        # h3
        if line.startswith("### "):
            content = line[4:].strip()
            html_lines.append(f'<h3 style="color:#2c3e50;margin-top:24px;margin-bottom:8px;">{content}</h3>')
        # h2
        elif line.startswith("## "):
            content = line[3:].strip()
            html_lines.append(f'<h2 style="color:#2c3e50;margin-top:28px;margin-bottom:10px;">{content}</h2>')
        # horizontal rule
        elif line.strip() == "---":
            html_lines.append('<hr style="border:none;border-top:1px solid #e0e0e0;margin:16px 0;">')
        # empty line
        elif line.strip() == "":
            html_lines.append("<br>")
        else:
            # bold
            processed = re.sub(r"\*\*(.+?)\*\*", r'<strong>\1</strong>', line)
            html_lines.append(f'<p style="margin:4px 0;line-height:1.6;">{processed}</p>')

    return "\n".join(html_lines)


def send_feedback(
    to_email: str,
    vendor_name: str,
    feedback_text: str,
    file_name: str,
    sender_name: str,
) -> None:
    """
    Send a feedback email via Gmail SMTP (SSL, port 465).
    Raises on failure.
    """
    if not config.GMAIL_USER or not config.GMAIL_APP_PASSWORD:
        raise RuntimeError("GMAIL_USER or GMAIL_APP_PASSWORD is not configured.")

    subject = f"Feedback de tu video: {file_name}"
    feedback_html = _markdown_to_html(feedback_text)

    html_body = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Feedback de Ventas</title>
</head>
<body style="margin:0;padding:0;background-color:#f4f6f8;font-family:Arial,Helvetica,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f6f8;">
    <tr>
      <td align="center" style="padding:32px 16px;">
        <table role="presentation" width="600" cellpadding="0" cellspacing="0"
               style="max-width:600px;width:100%;background:#ffffff;border-radius:8px;
                      box-shadow:0 2px 8px rgba(0,0,0,0.08);overflow:hidden;">

          <!-- Header -->
          <tr>
            <td style="background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);
                       padding:32px 40px;text-align:center;">
              <h1 style="margin:0;color:#ffffff;font-size:24px;font-weight:700;letter-spacing:-0.5px;">
                Feedback de tu Video de Ventas
              </h1>
              <p style="margin:8px 0 0;color:rgba(255,255,255,0.85);font-size:14px;">
                Análisis personalizado para <strong>{vendor_name}</strong>
              </p>
            </td>
          </tr>

          <!-- File info banner -->
          <tr>
            <td style="background:#f8f9fa;padding:12px 40px;border-bottom:1px solid #e9ecef;">
              <p style="margin:0;font-size:13px;color:#6c757d;">
                <strong>Archivo analizado:</strong> {file_name}
              </p>
            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="padding:32px 40px;color:#343a40;font-size:15px;line-height:1.6;">
              <p style="margin:0 0 20px;">Hola <strong>{vendor_name}</strong>,</p>
              <p style="margin:0 0 24px;color:#495057;">
                He analizado tu video de ventas y aquí tienes mi feedback detallado.
                Espero que te sea útil para seguir creciendo.
              </p>

              <div style="background:#f8f9fc;border-left:4px solid #667eea;
                          border-radius:0 6px 6px 0;padding:20px 24px;margin-bottom:24px;">
                {feedback_html}
              </div>

              <p style="margin:24px 0 0;color:#495057;">
                Estoy aquí para cualquier pregunta o si quieres profundizar en algún punto.
                ¡Sigue adelante!
              </p>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="background:#f8f9fa;padding:24px 40px;border-top:1px solid #e9ecef;
                       text-align:center;">
              <p style="margin:0;font-size:14px;color:#6c757d;">
                Un abrazo,<br>
                <strong style="color:#343a40;">{sender_name}</strong>
              </p>
              <p style="margin:12px 0 0;font-size:11px;color:#adb5bd;">
                Este feedback fue generado automáticamente por el Sistema de Feedback de Ventas.
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""

    # Plain text fallback
    plain_body = (
        f"Hola {vendor_name},\n\n"
        f"Aquí tienes el feedback de tu video: {file_name}\n\n"
        f"{feedback_text}\n\n"
        f"Un abrazo,\n{sender_name}"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{sender_name} <{config.GMAIL_USER}>"
    msg["To"] = to_email

    msg.attach(MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    context = ssl.create_default_context()
    logger.info("Sending email to %s (subject: %s)", to_email, subject)
    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            server.login(config.GMAIL_USER, config.GMAIL_APP_PASSWORD)
            server.sendmail(config.GMAIL_USER, to_email, msg.as_string())
        logger.info("Email sent successfully to %s", to_email)
    except smtplib.SMTPException as exc:
        raise RuntimeError(f"Failed to send email to {to_email}: {exc}") from exc
