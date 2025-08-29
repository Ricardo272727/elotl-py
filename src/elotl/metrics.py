import time 

class Timer:
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.duration = None
    
    def start(self):
        self.start_time = time.time() 
    
    def stop(self):
        self.end_time = time.time()

    def get_value(self):
        if not self.start_time or not self.end_time:
            raise ValueError("Invalid timer state")
        return (self.end_time - self.start_time) * 1000

