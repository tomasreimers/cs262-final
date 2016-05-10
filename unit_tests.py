from lib import Nexus
from lib import Worker
from lib import Resources
import unittest
import requests
import time
from multiprocessing import Process

# .then method for nexus
def print_result(result):
	print "*** Result of computation: ", result._value

"""
Function:
	Returns the sum of two integers
Args:
	a (int) - a number for adding
	b (int) - a number for adding
Returns:
	int
"""
def sum_numbers(a, b):
	return a + b

"""
Function:
	Counts how many times letter ch appears in string s
Args:
	s (string) - a string to count occurrences in
	ch (char)  - a letter to count occurrences of
Returns:
	int
"""
def count_occurrences(s, ch):
	count = 0
	for c in s:
		if c == ch:
			count += 1
	return count

def factorial(n):
	# error checking
	if n < 0:
		return -1
	elif n == 0:
		return 1
	else:
		to_return = 1
		for i in xrange(1, n + 1):
			to_return *= i
		return to_return

def output_of(name):
	def wrapped(result):
		print name, ":", result._value
	return wrapped

class TestRemoteFunctionExecution():
	"""
	How to run:
		1) execute python run_worker.py in a terminal window
		2) execute python nick-master-test.py in another terminal window
		3) check logs to see if computed result matches expected result
	"""

	def set_up(self):
		self.nexus = Nexus.Nexus()
		self.nexus.register_worker("http://localhost:5000/", "")

	"""
	Function call:
		sum(1,2)

	Expected print value:
		3
	"""
	def test_1(self):
		print "Running test #1"

		# prepare computation object being sent to client
		runnable = Resources.Runnable(sum_numbers, [1,2])
		computation = Resources.Computation(runnable)
		computation.then(print_result)

		# send work to a remote machine
		self.nexus.load_work(computation)

		# wait for result to come back
		self.nexus.wait()

	"""
	Function call:
		count_occurrences("apple apple apple apple", "a")
	Expected print value:
		4
	"""
	def test_2(self):
		print "Running test #2"

		# prepare computation object being sent to client
		runnable = Resources.Runnable(count_occurrences, ["apple apple apple apple", "a"])
		computation = Resources.Computation(runnable)
		computation.then(print_result)

		# send work to a remote machine
		self.nexus.load_work(computation)

		# wait for result to come back
		self.nexus.wait()

	"""
	Function call(s):
		factorial(4)
		factorial(5)
		factorial(6)
	Expected print value:
		24
		120
		720
	"""
	def test_3(self):
		print "Running test #3"

		sfactorial = self.nexus.shard(factorial)

		sfactorial(4).then(output_of("4!"))
		sfactorial(5).then(output_of("5!"))
		sfactorial(6).then(output_of("6!"))
		sfactorial(100).then(output_of("100!"))


	def test_4(self):
		print "Running test #4"

		@self.nexus.shard
		def derp(foo):
			return foo + 5

		derp(10).then(output_of("Derp(10)"))

		self.nexus.wait()

	def test_5(self):
		print "Running test #5"

		@self.nexus.shard
		def raise_exception():
			return 10 / 0

		def check_exception(result):
			print result._is_exception
			print result._value

			result.evaluate()

		raise_exception().then(check_exception)

		self.nexus.wait()

if __name__ == '__main__':
	tester = TestRemoteFunctionExecution()
	tester.set_up()
	tester.test_1()
	tester.test_2()
	tester.test_3()
	tester.test_4()
	tester.test_5()
