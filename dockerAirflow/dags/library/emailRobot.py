import email.header
import email.mime
import email.mime.text
import getpass, imaplib, smtplib, os, sys
import json, re, random
import email
import poplib   
import psycopg2
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from datetime import date, datetime
from pathlib import Path

#=============================================================================#
#=============================== EMAIL_ROBOT v0.0 ============================#
#=============================================================================#
# email_robot class is used to handle all action related to receiving of      # 
# report from email and sending all notification after processing of reports. #
# take config file as a parameter for initialisation                          #
# It has two main or 'public' method:                                         #
#   1. fetch_emails, which technically get emails with reports from           # 
#      email server and insert them in to db for downstream systems to        # 
#      process;                                                               #
#   2. send_validation_results, which gets protocols after that indicate      #
#      reports' validation results and send them back to sender               #   
###############################################################################
class email_robot():
    def __init__(self, conf:json=None):
        self.robot_ID = f'{str(random.randint(100,999))}_{str(datetime.now())}'
        if (conf is None):
            txt = f'#>{datetime.now()}_User@StatDep: Failed to crate '
            txt = txt + f'messageEmailRobot: valid config is not privided' 
            print (txt)
        self.fetch_protocol = str(conf["fetch_protocol"]).lower()
        if self.fetch_protocol == 'pop3':
            self.host = conf["pop3_email_host"]
            self.port = conf["pop3_email_port"]
        if self.fetch_protocol == 'imap':
            self.host = conf['imap_email_host']
            self.port = conf['imap_email_port']
        self.smtp_host = conf['smtp_email_host']
        self.smtp_port = conf['smtp_email_port']
        self.login = conf["login"]
        self.password = conf["password"]
        self.max_emails_to_fetch = conf["max_emails_to_fetch"] 
        self.allowed_file_formats = conf["allowed_file_formats"]
        self.file_name_pattern = conf["file_name_pattern"]
        self.db_host = conf["db_host"]
        self.db_port = conf["db_port"]
        self.db_name = conf["db_name"]
        self.db_user = conf["db_user"]
        self.db_pass = conf["db_pass"]

        print (f'#>{datetime.now()}_User@StatDep: EmailRobot in screated.') 

    def fetch_emails (self):
        txt = f'#>{datetime.now()}_User@StatDep: get_for_recent_emails '
        txt = txt + f'is started...'
        print (txt)
        if (self.fetch_protocol == "pop3"):
            return self.pop3_fetch_emails()
        if (self.fetch_protocol == "imap"):
            return self.imap_fetch_emails()
        txt = f'#>{datetime.now()}_User@StatDep: get_for_recent_emails '
        txt = txt + f'is successful.'
        print (txt) 

    # TODO: implement when neeeded.
    def imap_fetch_emails (self):
        print ("The method is not implemented!!!")

    def pop3_fetch_emails (self):
        txt = f'#>{datetime.now()}_User@StatDep: pop3_fetch_emails is'
        txt = txt + f'started...'
        print (txt)
        mail = poplib.POP3_SSL(self.host)
        print (f'   {mail.getwelcome()}')
        mail.user (self.login)
        mail.pass_(self.password)

        # Maximum emails to fetch per request
        emails_number = len(mail.list()[1])
        txt = f'   mailbox have {emails_number} emails and the robot '
        txt = txt + f'can process {self.max_emails_to_fetch}'
        print (txt)
        if emails_number == 0:
            txt = f'#>{datetime.now()}_User@StatDep: no emails to '
            txt = txt + f'process, exiting pop3_fetch_emails()...'
            print (txt)
            return
        if emails_number > self.max_emails_to_fetch: 
            emails_number = self.max_emails_to_fetch

        # iterate throught each email and each attachment in these emails      #
        for i in range(1, emails_number + 1):
            em = email.message_from_bytes(b'\n'.join(mail.retr(i)[1]))
            if em.is_multipart():
                raw_attachments = self.get_proper_attachements(em) 
                if raw_attachments:       
                    uidl = mail.uidl(i).decode("UTF-8")
                    fetch_ID = self.login + '_' +  self.robot_ID + '_' + \
                               em['Message-ID']
                    dt = email.utils.parsedate_tz(em["Date"])
                    email_datetime = \
                                datetime(dt[0],dt[1],dt[2],dt[3],dt[4],dt[5]).\
                                strftime(r'%Y-%m-%d %H:%M:%S')
                    sender = self.decode_mime_words(em["From"])
                    uploaded_datetime = datetime.now().\
                                        strftime(r'%Y-%m-%d %H:%M:%S')
                    attachment_name = None 
                    print_massage = None
                    status = None
                    channel = 1
                    upload_status = 0

                    # Interate throught attachemts and insert them two tables
                    # such that two tables form one-to-one relationships
                    for att in raw_attachments:
                        files_status = 0
                        attachment_name = att[0]
                        attachment = bytes(att[1])
                        if not self.is_attachment_name_valid(att[0]): 
                            upload_status = 5
                            files_status = 5
                        else:
                            upload_status = 1
                            files_status  = 1
                        query = "WITH uploads_id (id) AS (INSERT INTO \
                                sma_stat_dep.tbl_file_upload \
                                (UID, fetch_id, email_datetime \
                                , uploaded_datetime, email_from\
                                , upload_status, channel) \
                                VALUES (%s, %s, %s, %s, %s, %s, %s)\
                                RETURNING id) INSERT INTO \
                                sma_stat_dep.tbl_files (id_file_upload \
                                , upload_status, file_name, file  )\
                                SELECT id, %s, %s, %s  from uploads_id;"  
                        insert_values = [uidl, fetch_ID, email_datetime\
                                        , uploaded_datetime, sender \
                                        , upload_status, channel \
                                        , files_status, attachment_name \
                                        , attachment]                                    
                        pnt_msg = f'   Email from {email_datetime}'
                        pnt_msg = pnt_msg + f',  {sender}  with statis '
                        pnt_msg = pnt_msg + f'{upload_status} and '
                        pnt_msg = pnt_msg + f'attachement name {att[0]} '
                        pnt_msg = pnt_msg + f'uploaded to db'
            # Inserting into database 
                        with psycopg2.connect(dbname=self.db_name, 
                                user=self.db_user, password=self.db_pass, \
                                host=self.db_host) as conn:
                            print (f'   #>{datetime.now()}_conneciton to db ' 
                                   'is set.') 
                            with conn.cursor() as cursor:
                                cursor.execute (query, insert_values)
                                print (f'   #>{datetime.now()}_cursor '
                                       'execution is successful.')
                    #TODO try catch here
            # printing to log            
                        print (pnt_msg)
      
            print (f'   deletion of email after inserting into database '
                    f'{mail.dele (i).decode("UTF-8")}')
        mail.quit()
        print (f'#>{datetime.now()}_User@StatDep: pop3_fetch_emails is '
                'successful.')
        return

    def get_proper_attachements(self, email_object): 
    # return allowed attachments of given email object as a list of tuples    #
    # tuple (status_of_upload as boolean, file_name as string, attachments as #
    # file)
        if email_object.is_multipart(): 
            # [print (part.get_content_type()) for part in email_object.walk()]
            email_attachments = [
                (self.decode_mime_words(part.get_filename()) \
                                        , part.get_payload(decode=True)) 
                for part in email_object.walk() 
                if part.get_content_type() in self.allowed_file_formats]
        else: return None
        if not email_attachments: return None
        else: return email_attachments

    def is_attachment_name_valid(self, file_name: str) -> bool: 
    # Check the name of string again regular expression and return            #
    # result in boolean                                                       #
        if (re.compile (self.file_name_pattern).match(file_name) is not None)\
           and \
           len (re.compile (self.file_name_pattern).match(file_name).group())\
           == len(file_name):
            return True
        return False

    def send_validation_results (self, max_records_to_fetch: int=None):    
    # Getting validation results from two tables of db:                       #
        print (f'#>{datetime.now()}_User@StatDep: send_validation_results is '
                      'started...')
        if max_records_to_fetch is None: max_records_to_fetch \
            = self.max_emails_to_fetch
        IN_PROGRESS = 2
        IMPORTED = 3
        FINISHED = 4
        REJECTED = 5
        get_uploads_query = "SELECT id, email_datetime, email_from \
                            , upload_status FROM sma_stat_dep.tbl_file_upload \
                            WHERE channel=1 AND upload_status=%s OR \
                            upload_status=%s LIMIT %s;"
        get_files_query   = "SELECT id, id_file_upload, file_name, logs \
                            FROM sma_stat_dep.tbl_files \
                            WHERE id_file_upload IN %s"
        updt_upload_query = "UPDATE sma_stat_dep.tbl_file_upload \
                            SET upload_status=%s WHERE id IN %s"

        with psycopg2.connect(dbname=self.db_name, user=self.db_user, 
                              password=self.db_pass, 
                              host=self.db_host) as conn:
            print (f'   #>{datetime.now()}_conneciton to db is set.') 
            with conn.cursor() as cursor:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
                cursor.execute(get_uploads_query, (REJECTED, IMPORTED \
                                , max_records_to_fetch))
                print (f'   #>{datetime.now()}_cursor execution is '
                                'successful.')
                # Getting information for files submission from 
                # tbl_file_upload
                upload_data = cursor.fetchall()
                # when there is not sumbissions to respond
                if not upload_data:
                    print (f'   There is no any new submissions found. The '
                            'get_validation_results is over.') 
                    return                     
                print ('   extracted information for about report sumbissions '
                        'found...')
                # changing status of the record to avoid parallel processing
                upload_ids = [x[0] for x in upload_data]
                cursor.execute(updt_upload_query \
                               , (IN_PROGRESS,tuple(upload_ids)))
                cursor.execute(get_files_query, (tuple(upload_ids),))
                files_data =  cursor.fetchall()

                # send validation results for each files uploaded
                for file_record in files_data:
                    file_upl_id = file_record[1]
                    file_name = file_record [2]
                    validation_logs= file_record[3]
                    
                    for upload in upload_data:
                        if (upload[0]!=file_upl_id): continue 
                        else:
                            email_datetime = upload[1]
                            address = upload[2]
                            upload_status = upload[3]
                            break
            
                    if upload_status == 5:
                        topic = f'omor@nbt.tj: файл "{file_name}" загруженный в'
                        topic = topic + f' {email_datetime} НЕ ПРИНЯТ'
                        bdy =  f'Здравствуйте, \n К сожалению файл'
                        bdy = bdy + f' с именем {file_name}, загруженный в '
                        bdy = bdy + f'{email_datetime} не был принят '
                        bdy = bdy + 'системой автоматической обработки. '
                        bdy = bdy + 'Пожалуйста, проверьте заполнение файла. '
                        bdy = bdy + '\n Дополнительные сведенья по обнаруженным'
                        bdy = bdy + ' ошибкам приложенны к письму.\n '
                        bdy = bdy + 'C уважением, \n Департамент статистики '
                        bdy = bdy + 'и платежного баланса '
                    if upload_status == 3:
                        topic = f'omor@nbt.tj: файл "{file_name}", загруженный в '
                        topic = topic + f'{email_datetime}, ПРИНЯТ УСПЕШНО'
                        bdy = f'Здравствуйте, \n файл {file_name}, загруженный в '
                        bdy = bdy + f'{email_datetime}, был успешно проверен '
                        bdy = bdy + 'и принят системой автоматической обработки.' 
                        bdy = bdy + '\n C уважением, \n Департамент статистики '
                        bdy = bdy + 'и платежного баланса'    
                                       
                    self.send_message (address=address, subject=topic \
                                       , message=bdy \
                                       , attachment_name=file_name + \
                                                        '_protocol.txt' \
                                       , attachment=validation_logs)
                # turn submission status to "finished"
                print ('   finilizing processing for the reserved file '
                        'uploads...')
                cursor.execute(updt_upload_query \
                               , (FINISHED,tuple(upload_ids)))
                return 
        raise Exception (f'#>{datetime.now()}_User@StatDep: '
                        'get_validation_results has failed...')
        return


    def send_message(self, address:str, subject:str, message: str,\
                           attachment_name:str, attachment:str):
    # method send notificaiton to specific address from the user email adress #
    # thake recepient address, subject, body of the message, attachement      #
    # filename and attachement file content (string) as paramenters return    #
    # true in case of success, or false                                       #
        msg = MIMEMultipart()
        msg['Subject'] = subject 
        msg['From'] = self.login
        msg['To'] = address 
        msg.attach(MIMEText(message))

        # work with attachement 
        part = MIMEBase('application', "text/csv")
        if (attachment_name is not None and attachment is not None):
            part.set_payload(attachment, 'utf-8')
            email.encoders.encode_base64(part)
            part.add_header('content-disposition', 'attachment' \
                            , filename=(attachment_name))
            msg.attach(part)

        print (f'#>{datetime.now()}_User@StatDep: email object ready to be '
                'sent:')
        # with smtplib.SMTP(host=self.smtp_host, port=self.smtp_port )  
        # as smtp:
        with smtplib.SMTP_SSL(host=self.smtp_host, port=self.smtp_port ) \
             as smtp:
            smtp.set_debuglevel(1) #change to decrease amount of comments
            smtp.login(user = self.login, password= self.password)
            smtp.sendmail(from_addr= self.login, to_addrs=address \
                          , msg=msg.as_string())
            print (f'#>{datetime.now()}_User@StatDep: email successfuly sent')
            return True
        print (f'#>{datetime.now()}_User@StatDep: connecting to smtp server '
                'is not successful!')    
        return False

    def decode_mime_words(self, string): 
    # Function needed to decode cirilic symbols
        return u''.join(word.decode(encoding or 'utf8') \
               if isinstance(word, bytes) else word 
               for word, encoding in \
                                    email.header.decode_header(string))


if __name__ == "__main__": 
        # main code
    path = Path(__file__).parent.parent
    print (f'path of config file:  {path}')
    email_conf = json.load (open(path / ("config/email_conf.json")))
    robot = email_robot(email_conf)
    robot.fetch_emails()
    robot.send_validation_results()

