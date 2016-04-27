import Nexus
import Resources
import time

# .then method for nexus
def print_result(result):
   print result._value

# function to be sent to worker
# want to compute sum(1,2) -> 3
def sum_numbers(a,b):
	return a + b

if __name__ == '__main__':
	# prepare computation object being sent to client
	runnable = Resources.Runnable(sum_numbers, [1,2])
	computation = Resources.Computation(runnable)
	computation.then(print_result)

	# # create nexus, register worker, and send code to remote machine
	nexus = Nexus.Nexus()
	nexus.register_worker("http://localhost:5000/", "")
	# time.sleep(3)

	# # send work to a remote machine
	nexus.load_work(computation)
	nexus.unload_work()