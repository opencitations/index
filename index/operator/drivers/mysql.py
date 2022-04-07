from argparse import ArgumentParser
from re import S, match

from mysql.connector import connect, Error as MysqlError

from index.operator.driver import OperatorDriver

import time


class MysqlDriver(OperatorDriver):
    """It implements citation operator driver routines using
    mysql."""

    def set_args(self, arg_parser: ArgumentParser, config_file):
        arg_parser.add_argument(
            "-mysh",
            "--mysql_host",
            default=config_file["mysql_driver"]["host"],
            help="Mysql host to use for the operator driver.",
        )
        arg_parser.add_argument(
            "-mysp",
            "--mysql_port",
            default=config_file["mysql_driver"]["port"],
            help="Mysql port to use for the operator driver.",
        )
        arg_parser.add_argument(
            "-mysu",
            "--mysql_user",
            default=config_file["mysql_driver"]["user"],
            help="Mysql user to use for the operator driver.",
        )
        arg_parser.add_argument(
            "-mysp",
            "--mysql_password",
            default=config_file["mysql_driver"]["password"],
            help="Mysql password to use for the operator driver.",
        )
        arg_parser.add_argument(
            "-mysd",
            "--mysql_db",
            default=config_file["mysql_driver"]["db"],
            help="Mysql database to use for the operator driver.",
        )
        self._tables = config_file["mysql_driver"]["tables"]

    def init(self, mysql_host, mysql_port, mysql_user, mysql_password, db):
        """It opens a connection with the database to use in order
        to retrieve informations about oci and citation entities"""
        self.mysql = connect(
            host=mysql_host,
            port=mysql_port,
            database=db,
            user=mysql_user,
            password=mysql_password,
        )

    def __find_table(self, id):
        for key in self._tables:
            if id < key:
                return self._tables[key]
        return None

    def __get_doi(self, doi):
        doi = self.__doi_manager.normalise(doi, include_prefix=True)
        if doi is None or match("^doi:10\\..+/.+$", doi) is None:
            return None
        cached_value = self.__cache_doi.get(doi)
        if cached_value is None:
            cursor = self.mysql.cursor()
            table = self.__find_table(doi)[0]
            query = "SELECT * FROM %s WHERE doi = %s"
            cursor.execute(query, (table, doi))
            row = cursor.fetchone()
            doi_obj = {"doi": doi, "validity": row[1], "date": row[2]}
            self.__cache_doi.add(doi, doi_obj)
            return doi_obj
        else:
            return cached_value

    def share_orcid(self, citing, cited):
        cursor = self.mysql.cursor()
        query = "SELECT DISTINCT cited.orcid as orcid FROM orcid as citing, orcid as cited WHERE citing.doi = %s AND cited.doi = %s AND citing.orcid = cited.orcid"
        cursor.execute(query, citing, cited)
        return [row[0] for row in cursor.fetchall()]

    def share_issn(self, citing, cited):
        cursor = self.mysql.cursor()
        query = "SELECT DISTINCT cited.issn as issn FROM issn as citing, issn as cited WHERE citing.doi = %s AND cited.doi = %s AND citing.issn = cited.issn"
        cursor.execute(query, citing, cited)
        return [row[0] for row in cursor.fetchall()]

    def get_date(self, doi):
        doi = self.__get_doi(doi)
        if doi != None:
            return doi["date"]
        else:
            return None

    def oci_exists(self, oci):
        try:
            self.mysql.autocommit = False
            cursor = self.mysql.cursor()
            query = "INSERT IGNORE INTO oci (oci, created_at) VALUES (%s, %s)"
            cursor.execute(query, oci, str(time.time()))
            self.mysql.commit()
            return cursor.rowcount == 0
        except MysqlError as error:
            self.mysql.rollback()
        finally:
            self.mysql.autocommit = True

    def are_valid(self, citing, cited):
        doi_citing = self.__get_doi(citing)
        doi_cited = self.__get_doi(cited)
        result = (
            (not doi_citing is None)
            and doi_citing["validity"]
            and (not doi_cited is None)
            and doi_cited["validity"]
        )

        return result
