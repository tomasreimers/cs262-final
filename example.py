from lib import Nexus
from lib import Resources

print "Run 'python run_worker.py' in a separate terminal on the same machine"
print ""

# function that prints the ._value attribute
# of a computation object
def print_result(result):
	print "Result of computation: ", result._value

# function that returns the sum of 2 numbers
def sum_numbers(a, b):
	return a + b

# set up nexus
nexus = Nexus.Nexus()
nexus.register_worker("http://localhost:5000/", "")

# prepare computation object being sent to client
runnable = Resources.Runnable(sum_numbers, [1,2])
computation = Resources.Computation(runnable)
computation.then(print_result)

# send work to a remote machine
nexus.load_work(computation)
nexus.unload_work()

# wait for all jobs to complete before ending the python script
nexus.wait()
