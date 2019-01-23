from locust import HttpLocust, TaskSet, TaskSequence, seq_task, task
class UserBehavior(TaskSet):
    

    def on_start(self):
            self.client.get("/createProducer")
            self.client.get("/createConsumer")
        

    
    @seq_task(1)
    @task
    def push_data(self):
       self.client.get("/pushData")

    
    @seq_task(2)
    @task
    def get_data(self):
       self.client.get("/pullData")

   #  def on_stop(self):
   #      self.client.get("/killAllProducerConsumer")




class WebsiteUser(HttpLocust):
    task_set = UserBehavior
    max_wait=200
    min_wait=100




