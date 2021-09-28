import unittest

from index.oaoci.scholixcitationsource import ScholixCitationSource
from csv import DictReader

class CreateNewCitationsTest(unittest.TestCase):
    def setUp(self):
        self.__dir = "./index/test_data/scholix_dump"
        self.citation_source = ScholixCitationSource(self.__dir)

    def test_init(self):
        self.assertEqual(
            self.citation_source._ScholixCitationSource__files, 
            [
                './index/test_data/scholix_dump/1.scholix',
                './index/test_data/scholix_dump/2.scholix'
            ]
        )

    def test_next_citation_data(self):
        expected_values = [
            ["10.1007/978-1-137-49092-6_3", "10.1192/bjp.107.446.119"],
            ["10.1007/978-1-137-49092-6_4", "10.1177/1363461510374558"],
            ["10.1007/978-1-137-49092-6_4", "10.1016/s0041-3879(28)80007-9"],
            ["10.1007/978-1-137-49092-6_4", "10.1080/09668139008411881"]
        ]
        for value in expected_values:
            citing, cited, _, _, _ = self.citation_source.get_next_citation_data()
            self.assertEqual([citing, cited], value)
    
    def test_next_file(self):
        expected_values = [
            ["10.1007/978-1-137-49092-6_3", "10.1192/bjp.107.446.119"],
            ["10.1007/978-1-137-49092-6_4", "10.1177/1363461510374558"],
            ["10.1007/978-1-137-49092-6_4", "10.1016/s0041-3879(28)80007-9"],
            ["10.1007/978-1-137-49092-6_4", "10.1080/09668139008411881"],
            ["10.1080/10242422.2020.1786071", "10.1007/s13197-015-1920-2"],
            ["10.1080/10242422.2020.1786071", "10.1016/j.biortech.2005.11.017"],
            ["10.1080/10242422.2020.1786071", "10.1021/jf000167b"],
            ["10.1080/10242422.2020.1786071", "10.1556/achrom.21.2009.2.1"],
            ["10.1080/10242422.2020.1786071", "10.1016/s1369-703x(99)00014-5"],
            ["10.1080/10242422.2020.1786071", "10.1159/000106089"],
            ["10.1080/10242422.2020.1786071", "10.1021/jf970484r"],
            ["10.1080/10242422.2020.1786071", "10.1016/j.biortech.2005.03.016"]
        ]
        for value in expected_values:
            citing, cited, _, _, _ = self.citation_source.get_next_citation_data()
            self.assertEqual([citing, cited], value)

    def test_update_status_file(self):
        with open(self.citation_source.status_file, "r", encoding="utf8") as f:
            r = DictReader(f, fieldnames=("file", "line"))
            target = ""
            for line in r:
                target = line
            self.assertEqual(target["file"], './index/test_data/scholix_dump/1.scholix')
            self.assertEqual(target["line"], '297')
        
    def test_directory_completed(self):
        while(True):
            value = self.citation_source.get_next_citation_data()
            if value is None:
                break

if __name__ == '__main__':
    unittest.main()
