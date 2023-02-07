db_user=admin
db_name=dwh
db_host=0.0.0.0
backupfolder=./DB_DUMPS/$(date +%d-%m-%Y_%H-%M-%S)
# Сколько дней хранить файлы
keep_day=30
sqlfile=$backupfolder/database-$(date +%d-%m-%Y_%H-%M-%S).sql
zipfile=$backupfolder/database-$(date +%d-%m-%Y_%H-%M-%S).zip
mkdir -p $backupfolder

if pg_dump -U $db_user -h $db_host -p 5434 $db_name > $sqlfile ; then
   echo 'Sql dump created'
else
   echo 'pg_dump return non-zero code'
   exit
fi

if gzip -c $sqlfile > $zipfile; then
   echo 'The backup was successfully compressed'
else
   echo 'Error compressing backup'
   exit
fi
echo $sqlfile
echo $zipfile

find $backupfolder -mtime +$keep_day -delete