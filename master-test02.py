import Nexus
import Resources
import time

def print_result(result):
   print result._value

# get code to send to worker
with open("two_sum.py", "r") as code_file:
	code = code_file.read()

nexus = Nexus.Nexus()
nexus.register_worker("http://localhost:5000/", "")

time.sleep(3)

test_runnable = Resources.Runnable("Dummy code")
test_computation = Resources.Computation(test_runnable)
test_computation.then(print_result)

nexus.load_work(test_computation)
nexus.unload_work()

time.sleep(15)
