from argparse import ArgumentParser
from index.operator.drivers.mysql import MysqlDriver
from index.operator.operator import CitationOperator
from index.runtime.base import RuntimeBase

from mysql.connector import connect, Error as MysqlError

from kafka import KafkaConsumer, KafkaProducer

from index.source.source import CitationSource
from index.storer.citationstorer import CitationStorer


class FarmManager:
    def __init__(self, mysql_host, mysql_port, mysql_user, mysql_password, db):
        self.mysql = connect(
            host=mysql_host,
            port=mysql_port,
            database=db,
            user=mysql_user,
            password=mysql_password,
        )

    def get_state(self, oci):
        cursor = self.mysql.cursor()
        query = "SELECT * FROM farm_manager WHERE oci = %s"
        cursor.execute(query, oci)
        row = cursor.fetchone()
        return {"oci": oci, "state": row[1], "date": row[2]}

    def set_state(self, oci, state):
        cursor = self.mysql.cursor()
        query = "UPDATE farm_manager SET state = %s WHERE oci = %s"
        cursor.execute(query, (state, oci))
        self.mysql.commit()
        return cursor.rowcount


class ParallelFarm(RuntimeBase):
    """This execution mode allows the execution of cnc in parallel
    using a farm pattern implemented through apache kafka and a
    mysql server for supporting citation operations.
    """

    def __init__(self):
        """ParallelFarm Runtime constructor."""
        super().__init__("Farm")
        self._driver = MysqlDriver()

    def set_args(self, arg_parser: ArgumentParser, config_file):
        """It adds the following arguments to the arguments parser:
        - mode: type of execution among emitter, worker and collector
        - kh: kafka host
        - kp: kafka port
        - mysql arguments see MysqlDriver.set_args() for info

        Args:
            arg_parser (ArgumentParser): argument parser to set additional arguments
            config_file (dict): index configuration file values map
        """
        super().set_args(arg_parser, config_file)
        arg_parser.add_argument(
            "-m",
            "--mode",
            required=True,
            help="The type of execution among emitter, worker and collector.",
            choices=["emitter", "worker", "collector"],
        )
        self._driver.set_args(arg_parser, config_file)
        arg_parser.add_argument(
            "-kh",
            "--kafka_host",
            required=True,
            help="Kafka host.",
            default=config_file["farm"]["kafka"]["host"],
        )
        arg_parser.add_argument(
            "-kp",
            "--kafka_port",
            required=True,
            help="Kafka port.",
            type=int,
            default=config_file["farm"]["kafka"]["port"],
        )
        self._manager = FarmManager(
            config_file["mysql_driver"]["host"],
            config_file["mysql_driver"]["port"],
            config_file["mysql_driver"]["user"],
            config_file["mysql_driver"]["password"],
            config_file["mysql_driver"]["db"],
        )

    def init(self, args, config_file):
        """It initialize the required objects for the farm runtime:
        the mysql driver, the citation operator and the citation source.
        Not always all the three objects are initialized, in details:
        - Both the collector and the emitter need the driver and the citation operator.
        - Only the emitter initialize the citation source.
        - Only the collector initialize the citation storer.

        Args:
            args (dict): runtime arguments
            config_file (dict): index configuration file values map
        """
        super().init(args, config_file)
        if args.mode != "collector":
            self._driver.init(
                args.mysql_host,
                args.mysql_port,
                args.mysql_user,
                args.mysql_password,
                args.mysql_db,
            )
            self.logger.info("Mysql driver initialized.")

        if args.mode != "collector":
            self.__op = CitationOperator(
                self._driver,
                args.baseurl,
                args.agent,
                args.source,
                args.service,
                args.lookup,
                args.prefix,
            )
            if args.mode == "emitter":
                self.__source = CitationSource(args.input, self._parser, self.logger)
        else:
            self.__storer = CitationStorer(
                args.data,
                args.baseurl + "/" if not args.baseurl.endswith("/") else args.baseurl,
            )

    def __worker(self, args):
        producer = KafkaProducer(
            bootstrap_servers=args.kafka_host + ":" + str(args.kafka_port)
        )
        consumer = KafkaConsumer(
            "producer-topic",
            group_id="oc-workers",
            bootstrap_servers=args.kafka_host + ":" + str(args.kafka_port),
        )

        for next_citation in consumer:
            producer.send(
                "collector-topic",
                {"citation": self.__op.process(next_citation.data.citation)},
            )

        self.logger.info(
            "Errors in ids existence " + str(self.__op.error_in_ids_existence)
        )

    def __emitter(self, args):
        producer = KafkaProducer(
            bootstrap_servers=args.kafka_host + ":" + str(args.kafka_port)
        )

        citation_raw = self.__source.get_next_citation_data()
        while citation_raw != None:
            if not self.__op.exists(citation_raw):
                # TODO: Mark as processed
                producer.send("producer-topic", {"citation": citation_raw})
            citation_raw = self.__source.get_next_citation_data()

        self.logger.info(
            "Citation already present " + str(self.__op.citations_already_present)
        )

    def __collector(self, args):
        citations_added = 0
        consumer = KafkaConsumer(
            "collector-topic",
            group_id="oc-collectors",
            bootstrap_servers=args.kafka_host + ":" + str(args.kafka_port),
        )

        for next_citation in consumer:
            # Save the citation
            try:
                self.__storer.store_citation(next_citation.data.citation)
                self.manager
            except Exception:
                self.logger.exception("Could not save citation")
            else:
                citations_added += 1

            self.__storer.store_citation(next_citation.data.citation)

        self.logger.info("Citation added " + str(citations_added))

    def run(self, args, _):
        """It run an parallel farm node with the mode specified among
        emitter, worker and collector.

        Args:
            args (dict): parameter values for execution
            _ (dict): index configuration file values map
        """
        if args.mode == "collector":
            self.__collector(args)
        if args.mode == "emitter":
            self.__emitter(args)
        if args.mode == "worker":
            self.__worker(args)
