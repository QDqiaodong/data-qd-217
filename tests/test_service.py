import unittest
from fragment_reassembly import process


class FragmentTests(unittest.TestCase):
    def base(self):
        return {"fragments":[{"id":"f2","datagram_id":"d","offset":2,"more":False,"payload_hex":"6364"},{"id":"f1","datagram_id":"d","offset":0,"more":True,"payload_hex":"6162"}]}
    def test_reassembles_out_of_order(self):
        row=process(self.base())["datagrams"][0]
        self.assertEqual((row["status"],row["payload_hex"]),("complete","61626364"))
    def test_identical_overlap_is_allowed(self):
        data=self.base(); data["fragments"].append({"id":"f3","datagram_id":"d","offset":1,"more":True,"payload_hex":"6263"})
        self.assertEqual(process(data)["datagrams"][0]["status"],"complete")
    def test_conflicting_overlap_is_evidence(self):
        data=self.base(); data["fragments"].append({"id":"f3","datagram_id":"d","offset":1,"more":True,"payload_hex":"ff"})
        self.assertEqual(process(data)["datagrams"][0]["status"],"conflict")
    def test_gap_is_incomplete(self):
        data=self.base(); data["fragments"][1]["payload_hex"]="61"
        self.assertEqual(process(data)["datagrams"][0]["missing_offsets"],[1])
    def test_multiple_terminal_lengths_are_ambiguous(self):
        data=self.base(); data["fragments"].append({"id":"f3","datagram_id":"d","offset":4,"more":False,"payload_hex":"65"})
        self.assertEqual(process(data)["datagrams"][0]["status"],"missing_or_ambiguous_terminal")
    def test_bad_hex_fails(self):
        data=self.base(); data["fragments"][0]["payload_hex"]="zz"
        with self.assertRaises(ValueError): process(data)

if __name__=="__main__": unittest.main()
