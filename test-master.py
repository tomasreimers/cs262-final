import sharded
import time

def print_result(result):
   print result._value

nexus = sharded.Nexus()
nexus.register_worker("http://localhost:5000/", "")

time.sleep(3)

test_runnable = sharded.Runnable("Dummy code")
test_computation = sharded.Computation(test_runnable)
test_computation.then(print_result)

nexus.load_work(test_computation)
nexus.unload_work()

time.sleep(15)
