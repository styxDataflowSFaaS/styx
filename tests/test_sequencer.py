import unittest

from worker.run_func_payload import RunFuncPayload
from worker.sequencer.sequencer import Sequencer
from worker.sequencer.vanilla_sequencer import CalvinSequencer


class TestSequencer(unittest.TestCase):

    def test_sequencer(self):
        test_payload = RunFuncPayload(b'0', 0, 0, 'test', 0, 'test', (0, ), None, None)
        seq = Sequencer()
        seq.set_worker_id(1)
        seq.set_n_workers(2)
        seq.sequence(test_payload)
        seq.sequence(test_payload)
        seq.sequence(test_payload)
        seq.sequence(test_payload)
        epoch = seq.get_epoch()
        print(epoch)
        seq.increment_epoch(remote_t_counters=[seq.t_counter+1], aborted={1, 3}, logic_aborts_everywhere={3})
        seq.sequence(test_payload)
        epoch = seq.get_epoch()
        print(epoch)

    def test_vanilla_sequencer(self):
        test_payload1 = RunFuncPayload(b'0', 0, 0, 'test', 0, 'test', (0,), None, None)
        test_payload2 = RunFuncPayload(b'1', 1, 1, 'test', 1, 'test', (1,), None, None)
        test_payload3 = RunFuncPayload(b'2', 2, 2, 'test', 2, 'test', (2,), None, None)
        n1 = 3
        n2 = 3
        n3 = 3
        seq1 = CalvinSequencer()
        seq2 = CalvinSequencer()
        seq3 = CalvinSequencer()
        for _ in range(n1):
            seq1.sequence(test_payload1)
        for _ in range(n2):
            seq2.sequence(test_payload2)
        for _ in range(n3):
            seq3.sequence(test_payload3)
        pld1, s1 = seq1.get_epoch()
        pld2, s2 = seq2.get_epoch()
        pld3, s3 = seq3.get_epoch()
        payloads = {1: pld1, 2: pld2, 3: pld3}
        lens = {key: len(pld) for key, pld in payloads.items()}
        counters = {key: 0 for key in payloads.keys()}
        total_len = sum(lens.values())
        def generate_round_robin_idx():
            i = 0
            while i < total_len:
                for worker in lens.keys():
                    if counters[worker] < lens[worker]:
                        yield worker, counters[worker], i
                        i += 1
                        counters[worker] += 1

        rr_gen = generate_round_robin_idx()
        for _ in range(total_len):
            print(next(rr_gen))
