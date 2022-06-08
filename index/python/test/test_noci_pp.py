#!python
# Copyright (c) 2022 The OpenCitations Index Authors.
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import unittest
from os.path import join, exists
from oc.index.preprocessing.nih_pp import NIHPreProcessing
import shutil
import csv


class NOCIPPTest(unittest.TestCase):
    """This class aims at testing the methods of the classes NIHPreProcessing and ICiteMDPreProcessing."""

    def setUp(self):
        self.test_dir = join("index", "python", "test", "data")
        self.NIHPP = NIHPreProcessing()
        self.num_0 = 356
        self.num_1 = 8
        self.num_2 = 5
        self.num_3 = 300
        self.num_4 = 4

        # NIH-OCC data, for NOCI parser
        self.input_dir = join(self.test_dir, "noci_pp_dump_input")
        self.output_dir = join(self.test_dir, "noci_pp_dump_output")
        self.lines_sample = [
            [1, 14161139],
            [1, 14323813],
            [1, 4990046],
            [1, 4988806],
            [2, 4150960],
            [2, 4356257],
            [2, 4846745],
            [2, 4357832],
        ]
        self.headers = ["citing", "referenced"]

        # iCite Metadata, for NOCI glob
        self.input_md_dir = join(self.test_dir, "noci_md_pp_dump_input")
        self.output_md_dir = join(self.test_dir, "noci_md_pp_dump_output")

        self.headers_md = [
            "pmid",
            "doi",
            "title",
            "authors",
            "year",
            "journal",
            "cited_by",
            "references"
        ]

        self.lines_sample_md = [
            [1,"10.1016/0006-2944(75)90147-7","Formate assay in body fluids: application in methanol poisoning.","A B Makar, K E McMartin, M Palese, T R Tephly",1975,"Biochem Med","27354968 6430597 27548239 7055965 30849241 21923939 6525992 7004236 33554654 34013366 34176544 34122605 6875695 109089 6615550 7396890 31264500 518695 33134728 27574557 30612633 28159467 33825562 2920026 24058668 2219121 2566887 6915 2733395 2334642 21569229 21912457 32727301 8461035 7265415 18553624 17016 20870 941156 6767446 28002644 32820233 19454010 3112558 33872434 24589977 6383751 7361809 7471469 9750739 34142875 28851427 29460824 31371922 33561370 30186270 3754027 7205659 99844 405471 24004664 31123410 115004 6815832 8040258 23350017 25489175 32660571 3537623 728186 20184691 7165305 32733622 7230276 214088 3789485 6968503 3426949 27555514 6549198 31544580 6634838 32765117","4972128 4332837 13672941 14203183 14161139 14323813 4990046 4988806"],
            [2,"10.1016/0006-291x(75)90482-9","Delineation of the intimate details of the backbone conformation of pyridine nucleotide coenzymes in aqueous solution.","K S Bose, R H Sarma",1975,"Biochem Biophys Res Commun","6267127 26376 29458872 25548608 190032 22558138 26259654 31435170 28302598 21542697 26140007 890065 31544580 990401 39671","4150960 4356257 4846745 4357832 4683494 4414857 4337195 4747846 4266012 4147828 1111570 4366080 1133382 4364802 4709237 4733399 4379057 17742850"],
            [3,"10.1016/0006-291x(75)90498-2","Metal substitutions incarbonic anhydrase: a halide ion probe study.","R J Smith, R G Bryant",1975,"Biochem Biophys Res Commun","25624746 33776281 32427033 28053241 22558138 24349293 7022113 24775716 27767123 29897055 13818 23643052 12463","4632671 4977579 4992430 4958988 14417215 4992429 4621826 804171 4263471 4625501 4399045 4987292 4628675"],
            [4,"10.1016/0006-291x(75)90506-9","Effect of chloroquine on cultured fibroblasts: release of lysosomal hydrolases and inhibition of their uptake.","U N Wiesmann, S DiDonato, N N Herschkowitz",1975,"Biochem Biophys Res Commun","564972 8907731 7060838 6734624 31258218 7906514 27528195 19261 8216204 6221090 6365083 6870915 518687 2932104 7378072 28111290 7020765 6239657 1673037 895139 6817901 29684045 2937496 1660878 2961332 26308401 6284168 2543468 27136678 7138559 33597631 31536476 22305045 2452090 8295847 6793366 6615487 7151282 21816972 32663199 7342970 7057100 428466 7236683 2864350 6712986 6273196 31480869 27727286 21975914 476148 2933416 2530086 27146891 6228287 27328325 3827950 7190150 6279685 1015835 6276663 2730569 7314075 3877059 7165718 12763","13663253 4271529 5021451 4607946 4374680 14907713 4275378 806251 4972437 4345092 4606365 4364008"],
            [5,"10.1016/0006-291x(75)90508-2","Atomic models for the polypeptide backbones of myohemerythrin and hemerythrin.","W A Hendrickson, K B Ward",1975,"Biochem Biophys Res Commun","7118409 6768892 2619971 2190210 3380793 20577584 8372226 7012375 856811 678527 33255345 33973855 402092 7012894 1257769 861288 1061139 3681996","4882249 5059118 14834145 1056020 5509841"],
            [6,"10.1016/0006-291x(75)90518-5","Studies of oxygen binding energy to hemoglobin molecule.","Y W Chow, R Pietranico, A Mukerji",1975,"Biochem Biophys Res Commun","26668515 32953401 33134728 32470071 28601826 31567007 33094064","4645548 14794725 16587956 14328650 1120094"],
            [7,"10.1016/0006-2952(75)90020-9","Maturation of the adrenal medulla--IV. Effects of morphine.","T R Anderson, T A Slotkin",1975,"Biochem Pharmacol","31215303 9414029 18246 41093 21355 6294570 942452 6870481 27705745 2858585 980225 24006099 5098 6259468 30612633 27030978 1271281 3003477 7001 7791109 6713511 999726 6280102 33872136 1774133 33872434","17517355 15836459 15364646 18984078 20028328 5382448 18200670 4678819 17135164 15277068 12676044 15273078 20091597 5846992 11434754 17943819 17365137 18204101 18066129 17470951 1153082 15853441 16011454 22432689 14065927 15078424 16830309 21328257 18928139 15927926 1092305 11555193 19842218 15277090 22432635 21871125 17594991 5949627 22432564 14973986 11106086 14627335 12832592 21259185 4726566 15802416 21228727 4726565 15277076 19476742 21820672 15942912 22435354 16752934 16635970 15277109 22435615 16880367 18484782 6017359 19591529 19615308 11950176 18425916 22432518 15783241 19086008 12427501 16487688 16093238 22435415 18928148 4400965 4763252 12950418 17317655 18584321 18955223 4148818 10885091 4685445 16338199 16520304 1834807 15571467 18285310 4403908 4937070 12020172 21782365 19588398 15095142 17182489 15829470 22432729 235930 20370653"],
            [8,"10.1016/0006-2952(75)90029-5","Comparison between procaine and isocarboxazid metabolism in vitro by a liver microsomal amidase-esterase.","K Moroi, T Sato",1975,"Biochem Pharmacol","9224641 4004904 2756715 3245748 8743560 6745527 27034524 31695317 4059658 1876749 21846409 6712734 31632046 32005835 8218228 6875867 3706263 6612732 907716 7087253 3974403 463492 7181951 33977870 34121840 9586587 6770417 31664040 6123588 6239051 7295325 25584359 8654187 301955 537261 6126328 6772191 6494163 7203374","19701189 9014768 21102463 18587394 13249955 15480983 18618634 16747230 16228179 19903424 4429589 17968351 17504508 4610350 19462429 4346266 15537905 19805227 8780562 4284968 14907713 16863561 15157825 19453248 5066156 4650633 18145350 19898471 2925048 20445446 20197757 6053218 18719139 20483763 16610046 19817673 4741776 13734134 20256398 11385576 15812518 18849966 20839313 17068223 8587604 12851870 14292314 19966812 4972656 20024904 5150150 3396969 14042697 4239096"],
        ]

    def test_dump_split(self):
        if exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        self.assertFalse(exists(self.output_dir))
        self.NIHPP.dump_split(self.input_dir, self.output_dir, self.num_3)

        # checks that the output directory is generated in the process.
        self.assertTrue(exists(self.output_dir))

        # checks that the input lines where stored in 2 files, one containing 300 items and the other the remaining 56
        files = self.NIHPP.get_all_files(self.output_dir)
        len_files = len(files)
        self.assertEqual(len_files, 2)
        for idx, file in enumerate(files):
            with open(file, "r") as op_file:
                reader = csv.reader(op_file, delimiter=",")
                next(reader, None)
                len_lines = len(list(reader))
                self.assertTrue(len_lines == 300 or len_lines == 56)


    def test_icmd_split(self):
        if exists(self.output_md_dir):
            shutil.rmtree(self.output_md_dir)
        self.assertFalse(exists(self.output_md_dir))
        self.NIHPP.dump_split(self.input_md_dir, self.output_md_dir, self.num_3, filter_col=self.headers_md)

        # checks that the output directory is generated in the process.
        self.assertTrue(exists(self.output_md_dir))

        # checks that the input lines where stored in 2 files, one containing 300 items and the other the remaining 56
        files = self.NIHPP.get_all_files(self.output_md_dir)
        len_files = len(files)
        self.assertEqual(len_files, 2)
        for idx, file in enumerate(files):
            with open(file, "r") as op_file:
                reader = csv.reader(op_file, delimiter=",")
                next(reader, None)
                len_lines = len(list(reader))
                self.assertTrue(len_lines == 300 or len_lines == 56)

    def test_icmd_chunk_to_file(self):
        if exists(self.output_md_dir):
            shutil.rmtree(self.output_md_dir)
        self.assertFalse(exists(self.output_md_dir))
        self.NIHPP.chunk_to_file(
            self.num_1, self.num_4, self.output_md_dir, self.headers_md, self.lines_sample_md
        )

        # checks that chunk_to_file recreates the output directory
        self.assertTrue(exists(self.output_md_dir))

        # checks that the input lines are correctly stored in a file
        files = self.NIHPP.get_all_files(self.output_md_dir)
        self.assertGreater(len(files), 0)

        # CSVFile_2.csv : target number (num_4) is 4 and current number (num_1) is 8 --> 8%4 == 0,  8//4  == 2
        self.assertTrue(exists(join(self.output_md_dir, "CSVFile_2.csv")))

        # runs again the process using a current number which is not a multiple of the target number, i.e.: 5
        shutil.rmtree(self.output_md_dir)
        self.assertFalse(exists(self.output_md_dir))
        self.NIHPP.chunk_to_file(
            self.num_2, self.num_4, self.output_md_dir, self.headers_md, self.lines_sample_md
        )

        # checks that chunk_to_file recreates the output directory
        self.assertTrue(exists(self.output_md_dir))

        # checks that the input lines are correctly stored in a file
        files = self.NIHPP.get_all_files(self.output_md_dir)
        self.assertGreater(len(files), 0)

        # CSVFile_Rem.csv : target number (num_4) is 4 and current number (num_1) is 8 --> 8%4 == 0,  8//4  == 2
        self.assertTrue(exists(join(self.output_md_dir, "CSVFile_Rem.csv")))
