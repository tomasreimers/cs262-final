from lib import Nexus
from lib import Worker
import unittest
import requests
import time
from multiprocessing import Process

class TestWorkerStates(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        worker = Worker.Worker()
        cls.workerProcess = Process(target=worker.start, args=())
        cls.workerProcess.start()

    def test_heartbeat(self):
        for i in xrange(5):
            r = requests.get('http://localhost:5000/heartbeat')
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.text, "State ready")
            time.sleep(1)

    def test_computation(self):
        r = requests.get('http://localhost:5000/computation')
        self.assertEqual(r.text, "Job starts running")
    #
    #     time.sleep(1)
    #     r = requests.get('http://localhost:5000/computation')
    #     self.assertEqual(r.text, "Worker busy")
    #
    #     time.sleep(1)
    #     r = requests.get('http://localhost:5000/heartbeat')
    #     self.assertEqual(r.text, "State running")
    #
    #     time.sleep(10)
    #     r = requests.get('http://localhost:5000/heartbeat')
    #     self.assertEqual(r.text, "Dummy result")

    @classmethod
    def tearDownClass(cls):
        cls.workerProcess.terminate()
        cls.workerProcess.join()

if __name__ == '__main__':
    unittest.main()
