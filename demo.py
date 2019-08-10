import time
import json
import shlex
import random
import threading


class Flag(object):
    def __init__(self):
        self.flag = 'maintain'
        self.sensors = ['0']
        self.threads = []



class Listener(threading.Thread):
    def __init__(self, f, name='listener'):
        threading.Thread.__init__(self)

        self.name           = name
        self.flag           = f
        self.kill_signal    = False


    def run(self):
        while not self.kill_signal:
            try:
                line = input('command>')
                self.flag.flag, self.flag.sensors = self.parse_input(line)
                #flag = input()

            except KeyboardInterrupt:
                self.kill_signal = True

            except IndexError as error:
                pass

            except Exception as error:
                print(error)

    def parse_input(self, input):
        line = shlex.split(input)
        return line[0], line[1].split(' ')



class Sensor(threading.Thread):
    def __init__(self, name='sensor', init_val=250, delta=1, r_delta=0.01):
        threading.Thread.__init__(self)

        self.id = None
        self.name = name
        self.init = init_val
        self.state = 'maintain'
        self.kill_signal = False

        self.producer = None
        self.topic = None

        self.delta      = delta
        self.r_delta    = r_delta


    def run(self):
        while not self.kill_signal:
            self.init = self.init + self.get_flag()*1.5*random.random()
            msg = str(self.init)
            if self.producer is not None:
                if self.topic is None:
                    raise NotImplementedError('topic not specified')
                self.producer.send(self.topic, {'value':msg})
                time.sleep(0.5)
            else:
                print(msg)
                #if self.state == 'rise':
                #    print(self.state)


    def get_flag(self):
        if self.state == 'rise':
            return self.delta
        elif self.state == 'drop':
            return -1*self.delta
        else:
            return self.r_delta*random.randint(-1, 1)


    def set_id(self, id):
        self.id = str(id)


    def set_producer(self, p):
        if self.producer is not None:
            print('producer in {} already exists'.format(__class__))
        self.producer = p


    def set_topic(self, t):
        self.topic = t


class Controller(object):
    def __init__(self):

        self.sensors = []
        self.listener = None
        self.state   = 'maintain'
        self.threads = []
        self.lock = False


    def _sanity_check(self):
        if not self.sensors or not self.threads or not self.listener:
            print('sensors: {}'.format(self.sensors))
            print('listener: {}'.format(self.listener))
            print('threads: {}'.format(self.threads))
            raise NotImplementedError('attributes in "Controller not initiated"')


    def add_sensor(self, s):
        if not self.lock:
            self.sensors.append(s.id)
            self.threads.append(s)
            s.run()


    def setup_listener(self, l):
        if self.listener is not None:
            raise UserWarning('listener in Controller already exists')
        self.listener = l


    def start_listener(self, state, sensors):
        self.listener.start(state, sensors)


    def has_live_threads(self):
        return (True in [t.isAlive() for t in self.threads]) and self.listener.isAlive()


    def start(self):
        self.lock = True
        self._sanity_check()
        while self.has_live_threads():
            try:
                for t in self.threads:
                    if t is not None and t.isAlive():
                        t.join()
                if self.listener is not None and self.listener.isAlive():
                    self.listener.join()
                for sid in self.sensors:
                    sid.state = self.state

            except KeyboardInterrupt:
                for t in self.threads:
                    t.kill_signal = True
                self.listener.kill_signal = True

            except Exception:
                for t in self.threads:
                    t.kill_signal = True
                self.listener.kill_signal = True


class Choose_sensor(threading.Thread):
    def __init__(self, f):
        threading.Thread.__init__(self)

        self.flag = f
        self.kill_signal = False

    def run(self):
        while not self.kill_signal:
            #global flag
            #global threads
            #global sensors

            for s in self.flag.sensors:
                s = int(s)
                self.flag.threads[s].state = self.flag.flag



def has_live_threads(threads):
    return True in [t.isAlive() for t in threads]


if __name__ == '__main__':
    f = Flag()

    topic_1 = 'temperature'
    topic_2 = 'humidity'
    topic_3 = 'ph'
    topic_4 = 'rpm'

    # tempereature
    thread_1 = Sensor(init_val=250, delta=1, r_delta=0.01)
    #thread_1.set_producer(p1)
    #thread_1.set_topic(topic_1)
    thread_1.start()
    f.threads.append(thread_1)

    # humidity
    thread_2 = Sensor(init_val=60, delta=0.8, r_delta=0.01)
    #thread_2.set_producer(p2)
    #thread_2.set_topic(topic_2)
    thread_2.start()
    f.threads.append(thread_2)

    #ph
    thread_3 = Sensor(init_val=3, delta=0.1, r_delta=0.001)
    #thread_3.set_producer(p3)
    #thread_3.set_topic(topic_3)
    thread_3.start()
    f.threads.append(thread_3)

    #rpm
    thread_4 = Sensor(init_val=80, delta=0.4, r_delta=0.01)
    #thread_4.set_producer(p4)
    #thread_4.set_topic(topic_4)
    thread_4.start()
    f.threads.append(thread_4)

    


    thread_5 = Listener(f)
    thread_5.start()
    f.threads.append(thread_5)

    thread_6 = Choose_sensor(f)
    thread_6.start()
    f.threads.append(thread_6)


    while has_live_threads(f.threads):
        try:
            for t in f.threads:
                if t is not None and t.isAlive():
                    t.join()

        except KeyboardInterrupt:
            for t in f.threads:
                t.kill_signal = True

        except Exception:
            for t in f.threads:
                t.kill_signal = True


    for i, t in enumerate(f.threads):
        if not t.isAlive():
            print('thread#{} stopped'.format(i+1))

    exit(-1)


if __name__ == '__main__':
    from kafka import KafkaProducer

    f = Flag()

    

    p1 = KafkaProducer(
        bootstrap_servers='192.168.88.250:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    p2 = KafkaProducer(
        bootstrap_servers='192.168.88.250:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    p3 = KafkaProducer(
        bootstrap_servers='192.168.88.250:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    p4 = KafkaProducer(
        bootstrap_servers='192.168.88.250:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    topic_1 = 'temperature'
    topic_2 = 'humidity'
    topic_3 = 'ph'
    topic_4 = 'rpm'

    # tempereature
    thread_1 = Sensor(init_val=250, delta=1, r_delta=0.01)
    thread_1.set_producer(p1)
    thread_1.set_topic(topic_1)
    thread_1.start()
    f.threads.append(thread_1)

    # humidity
    thread_2 = Sensor(init_val=60, delta=0.8, r_delta=0.01)
    thread_2.set_producer(p2)
    thread_2.set_topic(topic_2)
    thread_2.start()
    f.threads.append(thread_2)

    #ph
    thread_3 = Sensor(init_val=3, delta=0.1, r_delta=0.001)
    thread_3.set_producer(p3)
    thread_3.set_topic(topic_3)
    thread_3.start()
    f.threads.append(thread_3)

    #rpm
    thread_4 = Sensor(init_val=80, delta=0.4, r_delta=0.01)
    thread_4.set_producer(p4)
    thread_4.set_topic(topic_4)
    thread_4.start()
    f.threads.append(thread_4)

    


    thread_5 = Listener(f)
    thread_5.start()
    f.threads.append(thread_5)

    thread_6 = Choose_sensor(f)
    thread_6.start()
    f.threads.append(thread_6)


    while has_live_threads(f.threads):
        try:
            for t in f.threads:
                if t is not None and t.isAlive():
                    t.join()

        except KeyboardInterrupt:
            for t in f.threads:
                t.kill_signal = True

        except Exception:
            for t in f.threads:
                t.kill_signal = True


    for i, t in enumerate(f.threads):
        if not t.isAlive():
            print('thread#{} stopped'.format(i+1))







