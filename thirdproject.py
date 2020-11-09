from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import datetime as dt
import tweepy
import csv
import json
import re
import pendulum

d = dt.datetime.now()
local = pendulum.timezone("Asia/Jakarta")
default_args = {
	'owner' : 'Salman',
	'start_date' : dt.datetime(2020, 9, 4, tzinfo = local),
	'retries' :1,
	'concurrency': 5 
}

dag_conf = DAG('thirdproject', default_args=default_args, schedule_interval='0 */24 * * *', catchup=False)

def start() :
	print("ayo!!")

def finish():
	print("Job Anda sudah Selesai.")

def search_hashtags(consumer_key, consumer_secret, access_token, access_token_secret, hashtag_phrase):
    # buat autentikasi untuk akses twitter
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    # inisialisasi API
    api = tweepy.API(auth)

    fname = ''.join(re.findall(r"#(\w+)", hashtag_phrase)) #mencocokkan kata menggunakan regex, kata yang dicocokkan yaitu object hashtag
                                                           #\w artinya membaca semua huruf ataupun karakter
    with open(fname, 'w', encoding='utf8') as file: #'w' write data dan akan overwrite jika di running untuk selanjutnya, encoding membaca sama huruf huruf aneh
        w = csv.writer(file) 
        w.writerow(['date', 'tweet', 'username', 'all_hashtags', 'followers', 'languange'])
        
        for tweet in tweepy.Cursor(api.search, q=hashtag_phrase+' -filter:retweets', \
                                   tweet_mode='extended').items(1000): #extended menambah element pada iterable
            w.writerow([tweet.created_at, tweet.full_text.replace('\n',' '), tweet.user.screen_name.encode('utf-8'), [e['text'] for e in tweet._json['entities']['hashtags']], tweet.user.followers_count, tweet.lang])

def running ():
	access_token = ''     #access token sesuai akun twitter developer
	access_token_secret = '' #access token secret sesuai akun twit dev
	consumer_key = ''         #api key sesuai akun twitter developer 
	consumer_secret = ''      #api key secret akun twitter developer
	hashtag_phrase = '#COVID19'

	return search_hashtags(consumer_key, consumer_secret, access_token, access_token_secret, hashtag_phrase)

start_operator = PythonOperator(task_id='starting_job', python_callable=start, dag=dag_conf)
getdata_operator = PythonOperator(task_id='getdata_job', python_callable=running, dag=dag_conf)
cleansing_operator = BashOperator(task_id='cleansing_job', \
	bash_command='spark-submit --master yarn --class com.projek3.thirdProject /home/mancesalfarizi/selalu-1.0-SNAPSHOT.jar', dag=dag_conf)
finish_operator = PythonOperator(task_id='job_finished', python_callable=finish, dag=dag_conf)

start_operator >> getdata_operator >> cleansing_operator >> finish_operator	