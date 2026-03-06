from job_hive.queue import RedisQueue
from job_hive import HiveWork, Job
from job_hive import Group

work = HiveWork(queue=RedisQueue(name="test", host='192.168.11.157', password='yunhai'))


@work.delay_task()
def hello(index):
    print('你是', index)
    # 判断index是否为3，如果是3则抛出异常
    if index == 3:
        raise Exception('test')
    text = 'hello world {}'.format(index)
    return text


if __name__ == '__main__':
    group = Group(
        hello(1),
        hello(2),
        hello(3),
        hello(4),
        hello(5),
    )
    work.group_commit(group)
    work.work(result_ttl=30)