alarm_url="http://sms.kaolafm-no.com/notify/ddSendChatMsg?agentId=46033470&chatId=chat6e514b522ac39b9cf5fe70f305a5141b&senderId=06545830191128836485&content="
send_mail(){
    curl -d "title=$1&content=$2&receiver=$3&cc=$4" "http://sms.kaolafm-no.com/notify/sendMail"
}
ping(){
    java -jar /opt/recsys/scripts/recsys-zk-1.0.0-SNAPSHOT.jar ping $1 $2
}
point(){
    java -jar /opt/recsys/scripts/recsys-zk-1.0.0-SNAPSHOT.jar point $1 $2
}
