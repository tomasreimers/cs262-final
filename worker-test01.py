import Nexus
import Worker
import unittest
import requests
import time
from multiprocessing import Process

class TestWorkerStates(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        worker = Worker.Worker()
        p = Process(target=worker.start, args=())
        p.start()

    def test_heartbeat(self):
        print "HERERERE"
        r = requests.get('http://localhost:5000/heartbeat')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.text, "State ready")

        time.sleep(1)
        r = requests.get('http://localhost:5000/computation')
        self.assertEqual(r.text, "Job starts running")

        time.sleep(1)
        r = requests.get('http://localhost:5000/computation')
        self.assertEqual(r.text, "Worker busy")

        time.sleep(1)
        r = requests.get('http://localhost:5000/heartbeat')
        self.assertEqual(r.text, "State running")

        time.sleep(10)
        r = requests.get('http://localhost:5000/heartbeat')
        self.assertEqual(r.text, "Dummy result")

if __name__ == '__main__':
    unittest.main()
