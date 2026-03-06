from job_hive.queue import KafkaQueue
from job_hive import HiveWork

work = HiveWork(queue=KafkaQueue(
    topic_name='test',
    servers='1Panel-kafka-3wvJ:9092'
))


@work.task()
def hello(index):
    print('你是', index)


if __name__ == '__main__':
    hello(1)

    work.work()
