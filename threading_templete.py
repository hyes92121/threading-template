import threading 

class Controller(threading.Thread):
    def __init__(self):
        self.thread_pool = list()
    
    def add_thread(self, thread):
        self.thread_pool.append(thread)
    
    def has_live_threads(self):
        # check if there are any live threads in thread pool
        return True in [t.isAlive() for t in self.thread_pool]
    
    def start_all_threads(self):
        for t in self.thread_pool:
            t.start()
    
        while self.has_live_threads():
            try:
                for t in self.thread_pool:
                    if t is not None and t.isAlive():
                        t.join()
            except KeyboardInterrupt: # Ctrl-C termination of the program
                for t in self.thread_pool:
                    t.kill_signal = True
            except Exception as error:
                for t in self.thread_pool:
                    t.kill_signal = True
                raise error
                
class ThreadingClass(threading.Thread):
    def __init__(self, target=None, args=(), kwargs={}):
        """
        Discussion of super().__init__(self) vs Base.__init__(self):
        https://stackoverflow.com/questions/222877/what-does-super-do-in-python/33469090#33469090
        """
        threading.Thread.__init__(self) 
        self.kill_signal = False
        self.func = target
        self.args = args
        self.kwargs = kwargs

    def run(self):
        while not self.kill_signal:
            try:
                self.func(*self.args, **self.kwargs)
            except KeyboardInterrupt: # terminate the thread when Ctrl-C is pressed
                self.kill_signal = True


if __name__ == '__main__':
    ctrl = Controller()

    def x(tid, msg='Default message'):
        print(f'Message from thread {tid}: {msg}')

    t1 = ThreadingClass(target=x, args=((1, )), kwargs={'msg': 'Hello World', })
    t2 = ThreadingClass(target=x, args=((2, )), kwargs={'msg': 'FML', })

    ctrl.add_thread(t1)
    ctrl.add_thread(t2)

    ctrl.start_all_threads()



        


        
