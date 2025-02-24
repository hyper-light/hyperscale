import os
import time
import base64
from email.utils import make_msgid 


def get_attachment_content(file_path):
    with open(file_path, "rb") as file:
        file_data = file.read()
        base64_encoded = base64.b64encode(file_data)
        base64_encoded_str = base64_encoded.decode("utf-8")
        return base64_encoded_str

def guess_mime_type(file_path):
    # Một số kiểu MIME mặc định cho một số định dạng file phổ biến
    mime_types = {
        '.txt': 'text/plain',
        '.pdf': 'application/pdf',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.doc': 'application/msword',
        '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        '.zip': 'application/zip',
    }

    _, file_extension = os.path.splitext(file_path)

    return mime_types.get(file_extension.lower(), 'application/octet-stream')


def generate_email_content(to, from_, subject, message, attachment_file_list, cc_recipients):

    message_id = make_msgid()

    boundary = '------' + str(time.time()) + '------'
    email_content = ''
    email_content += f'Content-Type: multipart/mixed; boundary="{boundary}"\r\n'
    email_content += f"Message-ID: {message_id}\r\n"
    email_content += f'Date: {time.strftime("%a, %d %b %Y %H:%M:%S +0700", time.localtime())}\r\n'
    email_content += f'User-Agent: VMS\r\n'
    email_content += f'To: {to}\r\n'
    
    if len(cc_recipients) > 0:
        email_content += f'CC: {", ".join(cc_recipients)}\r\n'
    
    email_content += f'From: {from_}\r\n'
    email_content += f'Subject: {subject}\r\n'
    email_content += f'\r\n'
    email_content += f'--{boundary}\r\n'
    email_content += f'Content-Type: text/plain\r\n'
    email_content += f'\r\n'
    email_content += message

    if attachment_file_list:
        for attachment_path in attachment_file_list:
            attachment_content = get_attachment_content(attachment_path)
            mime_type = guess_mime_type(attachment_path)
            email_content += f'\r\n--{boundary}\r\n'
            email_content += f'Content-Type: {mime_type}\r\n'
            email_content += f'Content-Transfer-Encoding: base64\r\n'
            email_content += f'Content-Disposition: attachment; filename= "{os.path.basename(attachment_path)}"\r\n'
            email_content += f'\r\n'
            email_content += attachment_content

    email_content += f'\r\n--{boundary}--\r\n'

    return email_content