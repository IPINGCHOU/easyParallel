import multiprocessing
import time
from itertools import chain

class ParallelManager():
    def __init__(self, verbose = False, update_freq = 0.5):
        self.manager = multiprocessing.Manager()
        self.queue = self.manager.Queue()
        self.returns = self.manager.dict()
        # parameters for progress bar
        self.verbose = verbose
        self.update_freq = update_freq
        # self.lock = self.manager.Lock()

    def do_parallel(self, inputs, task_func, workers, order = True):
        self.input_len = len(inputs)
        self.worker_counts = workers
        self.workers = []
        self.order = order

        # prepare inputs
        for i, item in enumerate(inputs):
            self.queue.put([i, item])

        for w in range(workers):
            new_worker = multiprocessing.Process(target = self.worker_process, args = (task_func, self.queue))
            new_worker.start()
            self.workers.append(new_worker)

        # wait until all process ends
        while not self.queue.empty():
            if self.verbose:
                self.progress_bar_print()

        # make sure all workers ends its work
        for w in self.workers:
            w.join()

        # combine results
        return self.combine_results()

    def worker_process(self, task_func, share_queue):

        while True:
            try:
                # get task
                idx, task = share_queue.get(block = True, timeout = 0.5)
            except:
                return 

            # execute
            self.returns[str(idx)] = task_func(task)

    
    def edit_array(self, i):
        return self.returns[int(i)]

    def combine_results(self):
        
        # return with orders
        if self.order:
            returns = [self.returns[str(i)] for i in range(len(self.returns))]
            return returns
        
        else:
            return list(chain.from_iterable([*self.returns.values()]))
        
    def progress_bar_print(self):

        print('\r Now processed: {0} / {1}, update every {2} sec'.format(
            len(self.returns),
            self.input_len, 
            self.update_freq, end = '\r'))
        time.sleep(self.update_freq)

