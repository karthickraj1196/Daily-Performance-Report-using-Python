import mysql.connector
import pandas as pd
import xlrd
import ssl
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import datetime
db_host = input("DB_Machine IP")
username = input("UserName for DB")
passwd = input("Password for DB")
db_port = input("DB_Port")
db = mysql.connector.connect(host=db_host,user=username,password=passwd,port=db_port)
sql = input("Select query to pull the data from DB along with the file path") 
df = pd.read_sql(sql,db)
df.to_excel('data.xls',index=False)
data = pd.read_excel('data.xls')
#print(data.dtypes)
req = pd.DataFrame(data)
req['ROOT_PATH'] = '/opt/SharedData/Shared/'
#print(req)
req['FILE_PATH'] = req['ROOT_PATH'] + req['REQUEST_FILE_PATH']
#print(req)

output = pd.DataFrame(data)
output['TTT-Group_Count'] = '0'
output['PPT-Group_Count'] = '0'
output['Zone-Group_Count'] = '0'
output['Public-Transport_Count'] = '0'
output['Hired-Auto_Count'] = '0'
output['Non-owned Auto Count'] = '0'
#print(output)

i = 0 
for row in output.iterrows():
    fil = output.loc[i,'FILE_PATH']
    with open(fil) as f:
         contents = f.read()
         truck_count = contents.count("TruckDetail")
         psngr_count = contents.count("PrivatePassengerDetail")
         zone_count = contents.count("ZoneRatedDetail")
         public_count = contents.count("PublicTransportationDetail")
         auto_count = contents.count("HiredAutoDetail")
         non_own_count = contents.count("NonOwnedAutoDetail")
#         print(truck_count)
         output.at[i,'TTT-Group_Count'] = truck_count
         output.at[i,'PPT-Group_Count'] = psngr_count
         output.at[i,'Zone-Group_Count'] = zone_count
         output.at[i,'Public-Transport_Count'] = public_count
         output.at[i,'Hired-Auto_Count'] = auto_count
         output.at[i,'Non-owned_Count'] = non_own_count
    i = i + 1 
print(output)
output = output.drop(columns=['ROOT_PATH','REQUEST_FILE_PATH'])
output.to_excel('output.xls',index=False)

now = datetime.datetime.now()
server = smtplib.SMTP('smtp.office365.com','587')
server.starttls(context=ssl.create_default_context())
server.login("username","password")
recepients = ['recepient1', 'recepient2', 'recepient3']
msg = MIMEMultipart()
msg['From'] = "from address"
msg['To'] = ", ".join(recepients)
msg['Subject'] = "Subject Title" + now.strftime("%Y-%m-%d")
body = "Message Content"
msg.attach(MIMEText(body, 'plain'))
filename = "Name of file to be attached"
attachment = open("filepath", "rb")
p = MIMEBase('application', 'octet-stream')
p.set_payload((attachment).read())
encoders.encode_base64(p)
p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
msg.attach(p)
text = msg.as_string()
server.sendmail("from address", recepients, text)
server.quit()
