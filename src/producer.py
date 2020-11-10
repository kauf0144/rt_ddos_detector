import json
import re
import time
from collections import namedtuple
from kafka import KafkaProducer
from kafka.errors import KafkaError


def find_parens(s):
    s = s[s.index('(') + 1:]
    level = 1
    for match in re.finditer('[()]', s):
        level = level + (1 if match.group(0) == '(' else -1)
        if level == 0: return s[:match.start()]

# Need to read in data file...parse with regex and push to Kafka topic http_logs
def read_file(log):
    serialized_line = namedtuple('log_line',
                                 ['host', 'identity', 'user', 'time', 'request',
                                  'status', 'bytes', 'referer', 'full_user_agent'])
    re_format = re.compile(
        r"(?P<host>[\d\.]+)\s"
        r"(?P<identity>\S*)\s"
        r"(?P<user>\S*)\s"
        r"\[(?P<time>.*?)\]\s"
        r'"(?P<request>.*?)"\s'
        r"(?P<status>\d+)\s"
        r"(?P<bytes>\S*)\s"
        r'"(?P<referer>.*?)"\s'  # [SIC]
        r'"(?P<full_user_agent>.*?)"\s*'
    )
    while True:
        line = log.readline()
        match = re_format.match(line)
        if match:
            line_dict = serialized_line(**match.groupdict())._asdict()
            line_dict['os'] = find_parens(line_dict['full_user_agent'])
            yield json.dumps(line_dict)
        if not line:
            time.sleep(0.0001)
            continue

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')
    logfile = open("../resources/data_fill.txt", "r")
    loglines = read_file(logfile)
    for line in loglines:
        future = producer.send('http_logs', line.encode('utf-8'))
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as err:
            print(err)
            pass
