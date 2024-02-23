
from unittest import result
from neo4j import AuthToken, GraphDatabase, Query                         # The Neo4j python connectivity library "Neo4j Python Driver"
from neo4j import __version__ as neo4j_driver_version   # The version of the Neo4j driver being used
from neo4j.time import DateTime                         # to convert neo4j.time.DateTime's to python datetimes (and Dates)
import neo4j.graph                                      # To check returned data types
from .cypher_utils import CypherUtils, NodeSpecs        # Helper classes for NeoAccess
import math
import numpy as np
import pandas as pd
import pandas.core.dtypes.common
import os
import sys
import json
import time
from typing import Union, List, Tuple,Dict,Any

class NeptuneAccessCore:
    """
    A thin wrapper around the opencypher Python connectivity library for Amazon Neptune.
    It allows the execution of arbitrary opencypher queries and helps manage the returned data.

    SECTIONS IN THIS CLASS:
        * INIT (constructor) and DATABASE CONNECTION
        * RUNNING GENERIC OPENCYPHER QUERIES
    """

    def __init__(self,
                 host=os.getenv("NEPTUNE_HOST"),
                 port=8182,
                 credentials=(os.getenv("NEPTUNE_USER"), os.getenv("NEPTUNE_PASSWORD")),
                 debug=False,
                 autoconnect=True):
        """
        Initializes a new instance of the NeptuneAccess class.

        This function initializes a new instance of the NeptuneAccess class, which provides access to a Neptune database. It takes several optional parameters, including the host URL, port number, credentials, and flags for debug mode and autoconnect.

        Args:
            host (str, optional): The Neptune endpoint URL. Defaults to the value of the NEPTUNE_HOST environment variable.
            port (int, optional): The port number for connecting to Neptune. Defaults to 8182.
            credentials (tuple, optional): A pair of strings containing the database username and password. Defaults to the values of the NEPTUNE_USER and NEPTUNE_PASSWORD environment variables.
            debug (bool, optional): A flag indicating whether debug mode is to be used. If True, all the OpenCypher queries, and some additional info, will get printed. Defaults to False.
            autoconnect (bool, optional): A flag indicating whether the class should establish a connection to the database at initialization. Defaults to True.

        Raises:
            Exception: If unable to create a Neptune connection, an exception is raised reminding the user to check whether the Neptune database is running.
        """

        self.debug = debug                  # If True, all the OpenCypher queries, and some additional info,
                                            # will get printed

        self.block_query_execution = False  # If True, all the OpenCypher queries will get printed (just like done by debug),
                                            # but no database operations will actually be performed.
                                            # Caution: most functions will fail validations on the results
                                            #          of the query that wasn't executed. This option should probably
                                            #          be combined with an Exception catch

        self.host = host                    # Neptune endpoint URL
        self.port = port                    # Port number
        self.credentials = credentials

        self.graph = None

        if self.debug:
            print("~~~~~~~~~ Initializing NeptuneAccessCore object ~~~~~~~~~")

        if autoconnect:
            # Attempt to establish a connection to Neptune, and create a graph object
            self.connect()

    def connect(self) -> None:
        """
        Establishes a connection to the Neptune database.

        This function attempts to establish a connection to the Neptune database using the credentials stored in the object. If successful, it creates and saves a graph object for further interactions with the database.

        Returns:
            None

        Raises:
            Exception: If unable to create a Neptune connection, an exception is raised reminding the user to check whether the Neptune database is running.

        Example:
            neptune_access = NeptuneAccess(host="localhost", port=8182, credentials=("user", "password"))
            neptune_access.connect()
        """
        assert self.host, "Neptune endpoint must be specified in order to connect to the database"
        assert self.credentials, "Neptune database credentials (username and password) must be specified in order to connect to it"

        try:
            uri = f"bolt://{self.host}:{self.port}"

            username, password = self.credentials  # Unpack credentials
            if self.debug:
                print(f"Attempting to connect to Neptune host '{self.host}', with username '{username}'...")

            self.driver = GraphDatabase.driver(uri,auth=(username,password) ,encrypted=True)
            
            return self.driver

        except Exception as ex:
            error_msg = f"CHECK WHETHER Neptune IS RUNNING! While instantiating the NeptuneAccessCore object, it failed to create the graph: {ex}"
            raise Exception(error_msg)

    
    def version(self) -> str:
        """
        Returns the version of the NeptuneAccessCore class.

        This function returns the version number of the NeptuneAccessCore class as a string.

        Returns:
            str: The version number of the NeptuneAccessCore class.

        Example:
            neptune_access = NeptuneAccess(host="localhost", port=8182, credentials=("user", "password"))
            print(neptune_access.version())  # Outputs: '1.0.0'
        """
        return "4.4.11"  # or any version number you prefer
        
        

    def close(self) -> None:
        """
        Terminates the connection to the Neptune database.

        This function closes the connection to the Neptune database. It is automatically invoked after the last operation included in "with" statements.

        Returns:
            None

        Example:
            neptune_access = NeptuneAccess(host="localhost", port=8182, credentials=("user", "password"))
            neptune_access.connect()
            neptune_access.close()  # Closes the connection
        """
        if self.driver is not None:
            self.driver.close()
            print("Connection to Neptune database closed.")

    def query(self, cypher_query: str, data_binding=None, single_row=False, single_cell="", single_column=""):
        """
        Executes a Cypher query against the Neptune database and returns the result.

        This function executes the provided Cypher query (`cypher_query`) against the Neptune database. It can optionally use a dictionary (`data_binding`) for data binding in the query. The function can return the entire result, a single row, a single cell, or all values from a single column, depending on the parameters.

        Args:
            cypher_query (str): The Cypher query to execute.
            data_binding (dict, optional): A dictionary for data binding in the Cypher query. Defaults to None.
            single_row (bool, optional): If True, only the first row of the result is returned. Defaults to False.
            single_cell (str, optional): If provided, the value of the specified cell is returned. Defaults to "".
            single_column (str, optional): If provided, all values of the specified column are returned. Defaults to "".

        Returns:
            list or dict or None: The result of the Cypher query. The type and content of the result depend on the parameters.

        Example:
            cypher_query = "MATCH (n) RETURN n"
            result = neptune_access.query(cypher_query, single_row=True)
        """
        if self.debug or self.block_query_execution:
            self.debug_query_print(cypher_query, data_binding, method="query")
            if self.block_query_execution:
                return

        # Start a new session, use it, and then immediately close it
        with self.driver.session() as new_session:
            result = new_session.run(cypher_query, data_binding)

            # Note: A neo4j.Result object (printing it, shows an object of type "neo4j.work.result.Result")
            #       See https://neo4j.com/docs/api/python-driver/current/api.html#neo4j.Result
            if result is None:
                return []

            data_as_list = result.data()        # Return the result as a list of dictionaries.

        # Deal with empty result lists
        if len(data_as_list) == 0:  # If no results were produced
            if single_row:
                return None
            if single_cell:
                return None
            return []

        if single_row:
            return data_as_list[0]
        if single_cell:
            return data_as_list[0].get(single_cell)


        if not single_column:
            return data_as_list
        else:    # Useful in cases where one wants to return ALL the fields of a particular node returned by the query
            data = []
            for node in data_as_list:
                data.append(node[single_column])

        return data

        
    
    def test_dbase_connection(self) -> None:
        """
        Tests the connection to the Neptune database.

        This function attempts to perform a trivial OpenCypher query to validate whether a connection to the Neptune database is possible. A failure at start time is typically indicative of invalid credentials.

        Returns:
            None

        Raises:
            Exception: If unable to create a Neptune connection, an exception is raised reminding the user to check whether the Neptune database is running and the credentials are correct.

        Example:
            neptune_access = NeptuneAccess(host="localhost", port=8182, credentials=("user", "password"))
            neptune_access.test_dbase_connection()  # Raises an exception if the connection test fails
        """
        opencypher_query = "MATCH (n) RETURN n LIMIT 1"
        result=self.query(opencypher_query)
        print("connection successfull - ",result)
        
    
    def debug_query_print(self, q: str, data_binding=None, method=None) -> None:
        """
        Prints the given Cypher query, and optionally its data binding and/or the name of the calling method.

        This function is used for debugging purposes. It prints the provided Cypher query (`q`), and if provided, its data binding (`data_binding`) and the name of the calling method (`method`).

        Args:
            q (str): The Cypher query to print.
            data_binding (dict, optional): The data binding for the Cypher query. Defaults to None.
            method (str, optional): The name of the calling method. Defaults to None.

        Returns:
            None

        Example:
            debug_query_print(q="MATCH (n) RETURN n", data_binding={"name": "Alice"}, method="test_method")
        """
        if method:
            print(f"\nIn {method}().  Query:")
        else:
            print(f"Query:")

        print(f"    {q}")

        if data_binding:
            print("Data binding:")
            print(f"    {data_binding}")

        print()

    def query_extended(self, q: str, data_binding=None, flatten=False, fields_to_exclude=None) -> [dict]:
        """
        Executes a Cypher query against the Neptune database and returns extended information.

        This function is an extended version of `query()`. It is meant to extract additional information for queries that return Graph Data Types (i.e., nodes, relationships, or paths). It is useful in scenarios where nodes are returned, and their Neptune internal IDs and/or labels are desired in addition to all the properties and their values.

        Args:
            q (str): The Cypher query to execute.
            data_binding (dict, optional): A dictionary for data binding in the Cypher query. Defaults to None.
            flatten (bool, optional): A flag indicating whether the Graph Data Types need to remain clustered by record, or all placed in a single flattened list. Defaults to False.
            fields_to_exclude (list, optional): A list of strings with the names of fields that the user wishes to drop. No harm in listing fields that aren't present. Defaults to None.

        Returns:
            list: A (possibly empty) list of dictionaries if `flatten` is True, or a list of lists if `flatten` is False. Each item in the lists is a dictionary, with details that will depend on which Graph Data Types were returned in the Cypher query.

        Example:
            cypher_query = "MATCH (n) RETURN n"
            result = neptune_access.query_extended(cypher_query, flatten=True)
        """
        if self.debug or self.block_query_execution:
            self.debug_query_print(q, data_binding, method="query_extended")
            print(f"    flatten: {flatten} , fields_to_exclude: {fields_to_exclude}")
            if self.block_query_execution:
                return []

        # Start a new session, use it, and then immediately close it
        with self.driver.session() as new_session:
            result = new_session.run(q, data_binding)

            # Note: A neo4j.Result iterable object (printing it, shows an object of type "neo4j.work.result.Result")
            #       See https://neo4j.com/docs/api/python-driver/current/api.html#neo4j.Result
            if result is None:
                return []

            data_as_list = []

            # The following must be done inside the "with" block, while the session is still open
            for record in result:
                # Note: record is a neo4j.Record object - an immutable ordered collection of key-value pairs.
                #       (the keys are the dummy names used for the nodes, such as "n")
                #       See https://neo4j.com/docs/api/python-driver/current/api.html#record

                # EXAMPLE of record (if node n was returned):
                #       <Record n=<Node id=227 labels=frozenset({'person', 'client'}) properties={'gender': 'M', 'age': 99}>>
                #       (it has one key, "n")
                # EXAMPLE of record (if node n and node c were returned):
                #       <Record n=<Node id=227 labels=frozenset({'person', 'client'}) properties={'gender': 'M', 'age': 99}>
                #               c=<Node id=66 labels=frozenset({'car'}) properties={'color': 'blue'}>>
                #       (it has 2 keys, "n" and "c")

                data = []
                for item in record:
                    # Note: item is a neo4j.graph.Node object
                    #       OR a neo4j.graph.Relationship object
                    #       OR a neo4j.graph.Path object
                    #       See https://neo4j.com/docs/api/python-driver/current/api.html#node
                    #           https://neo4j.com/docs/api/python-driver/current/api.html#relationship
                    #           https://neo4j.com/docs/api/python-driver/current/api.html#path
                    # EXAMPLES of item:
                    #       <Node id=95 labels=frozenset({'car'}) properties={'color': 'white', 'make': 'Toyota'}>
                    #       <Relationship id=12 nodes=(<Node id=147 labels=frozenset() properties={}>, <Node id=150 labels=frozenset() properties={}>) type='bought_by' properties={'price': 7500}>

                    neo4j_properties = dict(item.items())   # EXAMPLE: {'gender': 'M', 'age': 99}

                    if isinstance(item, neo4j.graph.Node):
                        neo4j_properties["internal_id"] = item.id               # Example: 227
                        neo4j_properties["neo4j_labels"] = list(item.labels)    # Example: ['person', 'client']

                    elif isinstance(item, neo4j.graph.Relationship):
                        neo4j_properties["internal_id"] = item.id               # Example: 227
                        neo4j_properties["neo4j_start_node"] = item.start_node  # A neo4j.graph.Node object with "id", "labels" and "properties"
                        neo4j_properties["neo4j_end_node"] = item.end_node      # A neo4j.graph.Node object with "id", "labels" and "properties"
                        #   Example: <Node id=118 labels=frozenset({'car'}) properties={'color': 'white'}>
                        neo4j_properties["neo4j_type"] = item.type              # The name of the relationship

                    elif isinstance(item, neo4j.graph.Path):
                        neo4j_properties["neo4j_nodes"] = item.nodes            # The sequence of Node objects in this path

                    # Exclude any unwanted (ordinary or special) field
                    if fields_to_exclude:
                        for field in fields_to_exclude:
                            if field in neo4j_properties:
                                del neo4j_properties[field]

                    if flatten:
                        data_as_list.append(neo4j_properties)
                    else:
                        data.append(neo4j_properties)

                if not flatten:
                    data_as_list.append(data)

            return data_as_list

    def update_query(self, q: str, data_binding=None) -> dict:
        """
        Executes a Cypher query that updates the Neptune database and returns statistics about its actions.

        This function is typically used for queries that update the database. If the query returns any values, a list of them is also made available, as the value of the key 'returned_data'.

        Args:
            q (str): Any Cypher query, but typically one that doesn't return anything.
            data_binding (dict, optional): Data-binding dictionary for the Cypher query. Defaults to None.

        Returns:
            dict: A dictionary of statistics (counters) about the query just run. The keys include 'nodes_created', 'nodes_deleted', 'relationships_created', 'relationships_deleted', 'properties_set', 'labels_added', 'labels_removed', 'indexes_added', 'indexes_removed', 'constraints_added', 'constraints_removed', and 'returned_data'.

        Example:
            result = update_query("CREATE(n :CITY {name: 'San Francisco'}) RETURN id(n) AS internal_id")
            # result will be {'nodes_created': 1, 'properties_set': 1, 'labels_added': 1, 'returned_data': [{'internal_id': 123}]}
        """
        if self.debug or self.block_query_execution:
            self.debug_query_print(q, data_binding, method="update_query")
            if self.block_query_execution:
                 return {}

        # Start a new session, use it, and then immediately close it
        with self.driver.session() as new_session:
            result = new_session.run(q, data_binding)


            data_as_list = result.data()    # Fetch any data returned by the query, as a (possibly-empty) list of dictionaries

            info = result.consume()     # Get the stats of the query just executed
        
            if self.debug:
                print("    In update_query(). Attributes of ResultSummary object:")
                # Show as dictionary, which is available in info.__dict__
                for k, v in info.__dict__.items():
                    print(f"    {k} -> {v}")
                '''
                EXAMPLE of info.__dict__: 
                {   'metadata': { 
                                    'query': 'MATCH (n :`A` {`name`: $par_1}) DETACH DELETE n', 
                                    'parameters': {'par_1': 'Jill'}, 
                                    'server': <neo4j.api.ServerInfo object at 0x0000013AFFAF36A0>, 
                                    't_first': 0, 'fields': [], 'bookmark': 'FB:kcwQ7BUXt6dES3GUrMEnTGTC5ck+BZA=', 
                                    'stats': {'nodes-deleted': 1}, 'type': 'w', 't_last': 0, 'db': 'neo4j'
                                }, 
                    'server': <neo4j.api.ServerInfo object at 0x0000013AFFAF36A0>, 
                    'database': 'neo4j', 
                    'query': 'MATCH (n :`A` {`name`: $par_1}) DETACH DELETE n', 
                    'parameters': {'par_1': 'Jill'}, 
                    'query_type': 'w', 
                    'plan': None, 'profile': None, 
                    'notifications': None, 
                    'counters': {'nodes_deleted': 1}, 
                    'result_available_after': 0, 
                    'result_consumed_after': 0
                }
                '''
                
            output_data={}

            output_data['returned_data'] = data_as_list  # Add an extra entry to the dictionary, with the data returned by the query

            return output_data
        

class NeptuneAccess(NeptuneAccessCore):
    """
    IMPORTANT : for both Neo4j and Neptune database, the internal ID of a node is NOT guaranteed to be the same across different sessions.

    High-level class to interface with the Neo4j graph database from Python.

    Tested on version 4.4 of Neo4j Community version, but should work with other 4.x versions, too.
    NOT tested on any other major version of Neo4j; in particular, NOT tested with version 5

    This class is a layer above its parent class "NeptuneAccessCore",
        and it provides a higher-level functionality for common database operations,
        such as lookup, creation, deletion, modification, import, indices, etc.

    SECTIONS IN THIS CLASS:
        * INTERNAL DATABASE ID
        * RETRIEVE DATA
        * FOLLOW LINKS
        * CREATE NODES
        * DELETE NODES
        * MODIFY FIELDS
        * RELATIONSHIPS
        * LABELS
        * INDEXES
        * CONSTRAINTS
        * READ IN DATA from PANDAS
        * JSON IMPORT/EXPORT
        * DEBUGGING SUPPORT

    It makes use of separate classes (NOT meant for the end user) in the file cypher_utils.py
    """


    def assert_valid_internal_id(self, internal_id: int) -> None:
        """
        Validates the provided Neptune database internal ID.

        This function checks if the provided `internal_id` is a valid Neptune database internal ID. If not, it raises an exception.

        Args:
            internal_id (int): The Neptune database internal ID to validate.

        Returns:
            None

        Raises:
            Exception: If `internal_id` is not a valid Neptune database internal ID.

        Example:
            neptune_access.assert_valid_internal_id(123)  # Raises an exception if 123 is not a valid internal ID
        """
        CypherUtils.assert_valid_internal_id(internal_id)




    #####################################################################################################

    '''                                      ~   RETRIEVE DATA   ~                                            '''

    def ________RETRIEVE_DATA________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################


    def get_record_by_primary_key(self, labels: str, primary_key_name: str, primary_key_value,
                                      return_internal_id=False) -> Union[dict, None]:
        """
        Returns the first (and it ought to be only one) record with the given primary key, and the optional label(s),
        as a dictionary of all its attributes.

        If more than one record is found, an Exception is raised.
        If no record is found, return None.

        Args:
            labels (str): A string or list/tuple of strings. Use None if not to be included in search.
            primary_key_name (str): The name of the primary key by which to look the record up.
            primary_key_value: The desired value of the primary key.
            return_internal_id (bool, optional): If True, an extra entry is present in the dictionary, with the key "internal_id". Defaults to False.

        Returns:
            dict or None: A dictionary, if a unique record was found; or None if not found.

        Raises:
            Exception: If more than one record is found.

        Example:
            record = neptune_access.get_record_by_primary_key(labels="Person", primary_key_name="name", primary_key_value="Alice")
        """
        assert primary_key_name, \
            f"NeoAccess.get_record_by_primary_key(): the primary key name cannot be absent or empty (value: {primary_key_name})"

        assert primary_key_value is not None, \
            "NeoAccess.get_record_by_primary_key(): the primary key value cannot be None" # Note: 0 or "" could be legit

        match = self.match(labels=labels, key_name=primary_key_name, key_value=primary_key_value)
        result = self.get_nodes(match=match, return_internal_id=return_internal_id)

        if len(result) == 0:
            return None
        if len(result) > 1:
            raise Exception(f"NeoAccess.get_record_by_primary_key(): multiple records ({len(result)}) share the value (`{primary_key_value}`) in the primary key ({primary_key_name})")

        return result[0]



    def exists_by_key(self, labels: str, key_name: str, key_value) -> bool:
        """
        Checks if a node with the given labels and key_name/key_value exists in the Neptune database.

        This function returns True if a node with the specified labels and key_name/key_value exists in the Neptune database, and False otherwise.

        Args:
            labels (str): A string or list/tuple of strings representing the labels of the node.
            key_name (str): The name of the node attribute to check.
            key_value: The desired value of the key_name attribute.

        Returns:
            bool: True if a node with the given labels and key_name/key_value exists, False otherwise.

        Example:
            exists = neptune_access.exists_by_key(labels="Person", key_name="name", key_value="Alice")
        """
        record = self.get_record_by_primary_key(labels, key_name, key_value)

        if record is None:
            return False
        else:
            return True



    def exists_by_internal_id(self, internal_id: str) -> bool:
        """
        Checks if a node with the given internal ID exists in the Neptune database.

        This function returns True if a node with the specified internal ID exists in the Neptune database, and False otherwise.

        Args:
            internal_id (int): The internal ID of the node to check.

        Returns:
            bool: True if a node with the given internal ID exists, False otherwise.

        Example:
            exists = neptune_access.exists_by_internal_id(internal_id=123)
        """
        q = f"""
        MATCH (n)
        WHERE id(n) = '{internal_id}'
        return n
        """
        result = self.query(q)
        return True if result else False


    def count_nodes(self) -> int:
        """
        Computes and returns the total number of nodes in the Neptune database.

        This function queries the Neptune database to count the total number of nodes.

        Returns:
            int: The total number of nodes in the Neptune database.

        Example:
            total_nodes = neptune_access.count_nodes()
        """
        q = "MATCH (n) RETURN COUNT(n) AS number_nodes"

        return self.query(q, single_cell="number_nodes")



    def get_single_field(self, match: Union[int, NodeSpecs], field_name: str, order_by=None, limit=None) -> list:
        """
        Fetches a single field from nodes that match the specified conditions and returns a list of the field's values.

        This function is useful when you want to fetch just one field from a node or set of nodes and you want the values of that field in a list, rather than a dictionary of records.

        Args:
            match (Union[int, NodeSpecs]): Either an integer with an internal database node id, or a "NodeSpecs" object, as returned by match(), with data to identify a node or set of nodes.
            field_name (str): A string with the name of the desired field (attribute).
            order_by (str, optional): A string specifying the field to order the results by. Defaults to None.
            limit (int, optional): An integer specifying the maximum number of results to return. Defaults to None.

        Returns:
            list: A list of the values of the field_name attribute in the nodes that match the specified conditions.

        Example:
            field_values = neptune_access.get_single_field(match=123, field_name="name")
        """

        record_list = self.get_nodes(match=match, order_by=order_by, limit=limit)

        single_field_list = [record.get(field_name) for record in record_list]

        return single_field_list



    def get_nodes(self, match: Union[int, NodeSpecs],
                      return_internal_id=False, return_labels=False, order_by=None, limit=None,
                      single_row=False, single_cell=""):
        """
        Returns a list of the records corresponding to all the Neo4j nodes specified by the given match data.

        The records are returned as dictionaries of all the key/value node properties.

        Args:
            match (Union[int, NodeSpecs]): Either an integer with an internal database node id, or a "NodeSpecs" object, as returned by match(), with data to identify a node or set of nodes.
            return_internal_id (bool, optional): If True, the Neo4j internal node ID is included in the returned data. Defaults to False.
            return_labels (bool, optional): If True, the Neo4j label names are included in the returned data. Defaults to False.
            order_by (str, optional): A string specifying the field to order the results by. Defaults to None.
            limit (int, optional): An integer specifying the maximum number of results to return. Defaults to None.
            single_row (bool, optional): If True, only the first record is returned. If no record exists, None is returned. Defaults to False.
            single_cell (str, optional): If specified, the value of the field by that name in the first node is returned. Defaults to "".

        Returns:
            Union[list, dict, None]: If single_cell is specified, the value of the field by that name in the first node.
                                     If single_row is True, a dictionary with the information of the first record (or None if no record exists).
                                     Otherwise, a (possibly-empty) list whose entries are dictionaries with each record's information.

        Example:
            nodes = neptune_access.get_nodes(match=123, return_internal_id=True, return_labels=True)
        """
        match_structure = CypherUtils.process_match_structure(match, caller_method="get_nodes")

        if self.debug:
            print("In get_nodes()")
            print("    match_structure:", match_structure)

        # Unpack needed values from the match dictionary
        (node, where, data_binding, dummy_node_name) = match_structure.unpack_match()

        cypher = f"MATCH {node} {CypherUtils.prepare_where(where)} RETURN {dummy_node_name}"

        if order_by:
            cypher += f" ORDER BY n.{order_by}"

        if limit:
            cypher += f" LIMIT {limit}"


        # Note: the flatten=True takes care of returning just the fields of the matched node "n",
        #       rather than dictionaries indexes by "n"
        if return_internal_id and return_labels:
            result_list = self.query_extended(cypher, data_binding, flatten=True)
            # Note: query_extended() provides both 'internal_id' and 'neo4j_labels'
        elif return_internal_id:    # but not return_labels
            result_list = self.query_extended(cypher, data_binding, flatten=True, fields_to_exclude=['neo4j_labels'])
        elif return_labels:         # but not return_internal_id
            result_list = self.query_extended(cypher, data_binding, flatten=True, fields_to_exclude=['internal_id'])
        else:
            result_list = self.query_extended(cypher, data_binding, flatten=True, fields_to_exclude=['internal_id', 'neo4j_labels'])

        # Deal with empty result lists
        if len(result_list) == 0:   # If no results were produced
            if single_row:
                return None             # representing a record not found (different from a record with no fields, which will be {})
            if single_cell:
                return None             # representing a field not found
            return []

        # Note: we already checked that result_list isn't empty
        if single_row:
            return result_list[0]

        if single_cell:
            return result_list[0].get(single_cell)

        return result_list



    def get_df(self, match: Union[int, NodeSpecs], order_by=None, limit=None) -> pd.DataFrame:
        """
        Returns a Pandas DataFrame containing the records corresponding to all the Neo4j nodes specified by the given match data.

        This function is similar to get_nodes(), but with fewer arguments, and the result is returned as a Pandas DataFrame.

        Args:
            match (Union[int, NodeSpecs]): Either an integer with an internal database node id, or a "NodeSpecs" object, as returned by match(), with data to identify a node or set of nodes.
            order_by (str, optional): A string specifying the field to order the results by. Defaults to None.
            limit (int, optional): An integer specifying the maximum number of results to return. Defaults to None.

        Returns:
            pd.DataFrame: A DataFrame containing the records corresponding to all the Neo4j nodes specified by the given match data.

        Example:
            df = neptune_access.get_df(match=123)
        """
        result_list = self.get_nodes(match=match, order_by=order_by, limit=limit)
        return pd.DataFrame(result_list)



    def match(self, labels=None, internal_id=None,
              key_name=None, key_value=None, properties=None, clause=None, dummy_node_name="n") -> NodeSpecs:
        """
        Returns a "NodeSpecs" object storing all the passed specifications (the "RAW match structure"),
        as expected as argument in various other functions in this library,
        in order to identify a node or group of nodes.

        Args:
            labels (str or list/tuple of str, optional): One or more Neo4j labels. Defaults to None.
            internal_id (str, optional): The node's internal database ID. If specified, it overrides all the remaining arguments [except for the labels]. Defaults to None.
            key_name (str, optional): The name of a node attribute; if provided, key_value must be present, too. Defaults to None.
            key_value (str, optional): The required value for the above key; if provided, key_name must be present, too. Defaults to None.
            properties (dict, optional): A dictionary of property key/values pairs, indicating a condition to match. Defaults to None.
            clause (str or tuple/list of str and dict, optional): Either None, or a string containing a Cypher subquery, or a pair/list containing a Cypher subquery and the data-binding dictionary for it. Defaults to None.
            dummy_node_name (str, optional): A name by which to refer to the node; only used if a `clause` argument is passed. Defaults to "n".

        Returns:
            NodeSpecs: A python data dictionary, to preserve together all the passed arguments.

        Example:
            node_specs = neptune_access.match(labels="cars", key_name="make", key_value="Toyota")
        """
        return NodeSpecs(internal_id=internal_id,
                         labels=labels, key_name=key_name, key_value=key_value,
                         properties=properties, clause=clause, clause_dummy_name=dummy_node_name)



    def get_node_internal_id(self,labels: str=None, properties: Dict[str, Any]=None) -> str:
        """
        Returns the internal database ID of a single node identified by the provided labels and properties.

        This function constructs a Cypher query to match a node with the specified labels and properties, and returns its internal ID. If no node is found, or if more than one node is found, an exception is raised.

        Args:
            labels (str, optional): A string representing the labels of the node. Multiple labels should be separated by a colon (:). Defaults to None.
            properties (Dict[str, Any], optional): A dictionary representing the properties of the node. The keys should be property names and the values should be the corresponding property values. Defaults to None.

        Returns:
            str: The internal database ID of the matched node.

        Raises:
            AssertionError: If no node is found, or if more than one node is found.
        """
        
        cypher_query = "MATCH (n"
        if labels:
            cypher_query += f":{labels}"
        cypher_query += " {"
        if properties:
            cypher_query += ", ".join([f'{key}: "{value}"' for key, value in properties.items()])
        cypher_query += "}) RETURN id(n) AS internal_id"

        print("Cypher query:", cypher_query)


        print("cypher query - ",cypher_query)
        result = self.query(cypher_query)     

        assert len(result) != 0, "get_node_internal_id(): node NOT found"

        assert len(result) <= 1, f"get_node_internal_id(): node NOT uniquely identified ({len(result)} matches found)"

        print("node internal id result -  ",result)
        return result[0].get("internal_id")


# todo:start here
    #####################################################################################################

    '''                                 ~   CREATE NODES   ~                                          '''

    def ________CREATE_NODES________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################


    def create_node(self, labels: str, properties: dict = None) -> int:
        """
        Creates a new node with the given labels and properties.

        Args:
            labels (str): The label for the new node.
            properties (dict, optional): The properties for the new node. Defaults to None.

        Returns:
            int: The ID of the newly created node.

        Example:
            node_id = neptune_access.create_node(labels="Person", properties={'name': 'John', 'age': 30})
        """

        if properties is None:
            properties = {}

        # From the dictionary of attribute names/values,
        #       create a part of a Cypher query, with its accompanying data dictionary
        (attributes_str, data_dictionary) = CypherUtils.dict_to_cypher(properties)
        # EXAMPLE:
        #       attributes_str = '{`cost`: $par_1, `item description`: $par_2}'
        #       data_dictionary = {'par_1': 65.99, 'par_2': 'the "red" button'}

        # Turn labels (string or list/tuple of labels) into a string suitable for inclusion into Cypher
        cypher_labels = CypherUtils.prepare_labels(labels)

        # Assemble the complete Cypher query
        q = f"CREATE (n {cypher_labels} {attributes_str}) RETURN n"

        result_list = self.query_extended(q, data_dictionary, flatten=True) 
        if len(result_list) != 1:
            raise Exception("NeoAccess.create_node(): failed to create the requested new node")

        print("------------")
        print("create_node results ------",result_list)
        print("------------")
        return result_list[0]          # Return the Neo4j internal ID of the node just created



    def merge_node(self, labels, properties=None) -> dict:
        """
        Creates a new node with the given labels and properties only if no other node with the same labels and properties exists.

        Args:
            labels (str or list/tuple of str): A string, or list/tuple of strings, specifying Neo4j labels. It's acceptable to have blank spaces.
            properties (dict, optional): An optional (possibly empty or None) dictionary of properties to try to match in an existing node, or - if not found - to set in a new node. Defaults to None.
                Example: {'age': 22, 'gender': 'F'}

        Returns:
            dict: A dictionary with 2 keys: "created" (True if a new node was created, or False otherwise) and "internal_id".

        Example:
            result = neptune_access.merge_node(labels="Person", properties={'age': 22, 'gender': 'F'})
        """
        if properties is None:
            properties = {}

        # From the dictionary of attribute names/values,
        #       create a part of a Cypher query, with its accompanying data dictionary
        (attributes_str, data_dictionary) = CypherUtils.dict_to_cypher(properties)
        # EXAMPLE:
        #       attributes_str = '{`cost`: $par_1, `item description`: $par_2}'
        #       data_dictionary = {'par_1': 65.99, 'par_2': 'the "red" button'}

        # Turn labels (string or list/tuple of labels) into a string suitable for inclusion into Cypher
        cypher_labels = CypherUtils.prepare_labels(labels)

        # Assemble the complete Cypher query
        q = f"MERGE (n {cypher_labels} {attributes_str}) RETURN id(n) AS internal_id"


        result = self.update_query(q, data_dictionary)

        internal_id = result["returned_data"][0]["internal_id"]     # The internal database ID of the node found or just created

        if result.get("nodes_created", 0) == 1:
            return {"created": True, "internal_id": internal_id}
        else:
            return {"created": False, "internal_id": internal_id}



    def create_attached_node(self, labels, properties=None, attached_to=None, rel_name=None, rel_dir="OUT", merge=True) -> int:
        """
        Creates a new node with the given labels and properties, and attaches it to existing nodes specified in the 'attached_to' list using the given relationship name.

        Args:
            labels (str or list/tuple of str): Labels to assign to the newly-created node.
            properties (dict, optional): A dictionary of optional properties to assign to the newly-created node. Defaults to None.
            attached_to (int or list/tuple of int, optional): An integer, or list/tuple of integers, with internal database ID's to identify the existing nodes. Use None, or an empty list, to indicate if there aren't any. Defaults to None.
            rel_name (str, optional): Name of the newly created relationships. This is required, if an 'attached_to' list was provided. Defaults to None.
            rel_dir (str, optional): Either "OUT"(default), "IN" or "BOTH". Direction(s) of the relationships to create. Defaults to "OUT".
            merge (bool, optional): If True (default), a new node gets created only if there's no existing node with the same properties and labels. Defaults to True.

        Returns:
            int: An integer with the internal database ID of the newly-created node.

        Raises:
            Exception: If any of the requested link nodes isn't found, then no new node is created, and an Exception is raised.

        Example:
            node_id = neptune_access.create_attached_node(
                labels="COMPANY",
                properties={"name": "Acme Gadgets", "city": "Berkeley"},
                attached_to=[123, 456],
                rel_name="EMPLOYS"
            )
        """
        if type(attached_to) == str:
            attached_to = [attached_to]
        else:
            assert (attached_to is None) or (type(attached_to) == list) or (type(attached_to) == tuple), \
                f"create_attached_node(): the argument `attached_to` must be a list or tuple or None; instead, it's {type(attached_to)}"

        assert (rel_dir is None) or (rel_dir == "IN") or (rel_dir == "OUT"), \
            f"create_attached_node(): the argument `rel_dir` must be either 'IN' or 'OUT' or None; instead, it's `{rel_dir}`"

        if attached_to is not None:
            assert (rel_name is not None) and (rel_name != ""), \
                f"create_attached_node(): when the the argument `attached_to` is present, a non-empty `rel_name` must be passed"

        if self.debug:
            print(f"In create_attached_node().  labels: {labels}, properties: {properties}, "
                  f"attached_to: {attached_to}, rel_name: {rel_name}, rel_dir: {rel_dir}")

        if attached_to is None:
            links = None
        else:
            links = [{"internal_id": existing_node_id, "rel_name": rel_name, "rel_dir": rel_dir}
                     for existing_node_id in attached_to]
        
        print(f"In create_attached_node().  labels: {labels}, properties: {properties}, "
                f"attached_to: {attached_to}, rel_name: {rel_name}, rel_dir: {rel_dir}")

        return self.create_node_with_links(labels=labels, properties=properties, links=links, merge=merge)


    def create_node_with_links(self, labels, properties=None, links=None, merge=False) -> int:
        """
        Creates a new node with the given labels and properties, and links it to existing nodes specified in the 'links' list.

        Args:
            labels (str or list/tuple of str): Labels to assign to the newly-created node.
            properties (dict, optional): A dictionary of optional properties to assign to the newly-created node. Defaults to None.
            links (list of dict, optional): A list of dictionaries identifying existing nodes, specifying the name, direction and optional properties to give to the links connecting to them. Defaults to None.
                Each dict contains the following keys:
                    "internal_id" (int): Required to identify an existing node.
                    "rel_name" (str): Required, the name to give to the link.
                    "rel_dir" (str, optional): Either "IN" or "OUT" from the new node. Defaults to "OUT".
                    "rel_attrs" (dict, optional): A dictionary of relationship attributes.
            merge (bool, optional): If True, a new node gets created only if there's no existing node with the same properties and labels. Defaults to False.

        Returns:
            int: An integer with the Neo4j ID of the newly-created node.

        Raises:
            Exception: If any of the requested link nodes isn't found, then no new node is created, and an Exception is raised.

        Example:
            node_id = neptune_access.create_node_with_links(
                labels="PERSON",
                properties={"name": "Julian", "city": "Berkeley"},
                links=[
                    {"internal_id": 123, "rel_name": "LIVES IN"},
                    {"internal_id": 456, "rel_name": "EMPLOYS", "rel_dir": "IN"},
                    {"internal_id": 789, "rel_name": "OWNS", "rel_attrs": {"since": 2022}}
                ]
            )
        """
        if properties:
            properties = {k.replace(' ', '_'): v for k, v in properties.items()}

        cypher_query = "CREATE (n"
        if labels:
            if isinstance(labels, str):
                labels = [labels]
            cypher_query += ":" + ":".join(labels)
        if properties:
            cypher_query += " {"
            cypher_query += ", ".join([f"{k}: ${k}" for k in properties.keys()])
            cypher_query += "}"
        cypher_query += ")"

        if links:
            for link in links:
                cypher_query += f"\nWITH n\nMATCH (m) WHERE id(m) = '{link['internal_id']}'\n"
                rel_dir = link.get("rel_dir", "OUT")
                rel_name = link["rel_name"]
                rel_attrs = link.get("rel_attrs", {})
                # Format relationship attributes
                if rel_attrs:
                    rel_attrs_str = "{" + ", ".join([f"{k}: {v}" for k, v in rel_attrs.items()]) + "}"
                else:
                    rel_attrs_str = ""
                cypher_query += f"CREATE (n)-[:{rel_name} {rel_attrs_str}]->(m)" if rel_dir == "OUT" else f"CREATE (m)-[:{rel_name} {rel_attrs_str}]->(n)"

        print("cypher_query in create node with links - ",cypher_query)
        result=self.query(cypher_query, data_binding=properties)
        print("result in create node with links - ",result)
        return result


    def _assemble_query_for_linking(self, links: list) -> tuple:
        """
        Assembles the portions of a Cypher query to locate existing nodes and link them.

        This is a helper function for create_node_with_links() and potentially future methods.
        No query is actually run in this function.

        Args:
            links (list): A list of dictionaries identifying existing nodes and specifying the name, direction, and optional properties to give to the links connecting to them. 
                Each dict contains the following keys:
                    "internal_id" (int): Required to identify an existing node.
                    "rel_name" (str): Required, the name to give to the link.
                    "rel_dir" (str, optional): Either "IN" or "OUT" from the new node. Defaults to "OUT".
                    "rel_attrs" (dict, optional): A dictionary of relationship attributes.

        Returns:
            tuple: A 4-tuple containing the parts of the query and the needed data binding:
                1) q_MATCH
                2) q_WHERE
                3) q_MERGE
                4) data_binding

        Example:
            query_parts = neptune_access._assemble_query_for_linking(
                links=[
                    {"internal_id": 123, "rel_name": "LIVES IN"},
                    {"internal_id": 456, "rel_name": "EMPLOYS", "rel_dir": "IN"},
                    {"internal_id": 789, "rel_name": "OWNS", "rel_attrs": {"since": 2022}}
                ]
            )
        """

        assert links and type(links) == list and len(links) > 0, \
            f"NeoAccess._assemble_query_for_linking(): the argument must be a non-empty list"

        # Define the portion of the Cypher query to locate the existing nodes
        q_MATCH = "MATCH"
        q_WHERE = "WHERE"

        # Define the portion of the Cypher query to link up to any of the existing nodes
        q_MERGE = ""

        data_binding = {}
        for i, edge in enumerate(links):
            match_internal_id = edge.get("internal_id")
            if match_internal_id is None:    # Caution: it might be zero
                raise Exception(f"NeoAccess._assemble_query_for_linking(): Missing 'internal_id' key for the node to link to (in list element {edge})")

            assert type(match_internal_id) == int, \
                f"NeoAccess._assemble_query_for_linking(): The value of the 'internal_id' key must be an integer. The type was {type(match_internal_id)}"

            rel_name = edge.get("rel_name")
            if not rel_name:
                raise Exception(f"NeoAccess._assemble_query_for_linking(): Missing name ('rel_name' key) for the new relationship (in list element {edge})")

            node_dummy_name = f"ex{i}"  # EXAMPLE: "ex3".   The "ex" stands for "existing node"
            q_MATCH += f" (ex{i})"      # EXAMPLE: " (ex3)"

            q_WHERE += f" id({node_dummy_name}) = {match_internal_id}"   # EXAMPLE: " id(ex3) = 123"


            rel_dir = edge.get("rel_dir", "OUT")        # "OUT" is the default value
            rel_attrs = edge.get("rel_attrs", None)     # By default, no relationship attributes

            # Process the optional relationship properties
            (rel_attrs_str, cypher_dict_for_edge) = CypherUtils.dict_to_cypher(rel_attrs, prefix=f"EDGE{i}_")
            # EXAMPLE of rel_attrs_str:         '{since: $EDGE1_par_1}'  (possibly a blank string)
            # EXAMPLE of cypher_dict_for_edge:  {'EDGE1_par_1': 2021}    (possibly an empty dict)

            data_binding.update(cypher_dict_for_edge)           # Merge cypher_dict_for_edge into the data_binding dictionary

            if rel_dir == "OUT":
                q_MERGE += f"MERGE (n)-[:`{rel_name}` {rel_attrs_str}]->({node_dummy_name})"  # Form an OUT-bound connection
                # EXAMPLE of term:  "MERGE (n)-[:`OWNS` {since: $EDGE1_par_1}]->(ex1)"
            else:
                q_MERGE += f"MERGE (n)<-[:`{rel_name}` {rel_attrs_str}]-({node_dummy_name})"  # Form an IN-bound connection
                # EXAMPLE of term:  "MERGE (n)<-[:`EMPLOYS` ]-(ex0)"

            if i+1 < len(links):
                q_MATCH += ","          # Comma separator, except at the end
                q_WHERE += " AND"
                q_MERGE += "\n"
            # END for

        # EXAMPLE of q_MATCH at this stage; note that (ex0), etc, refer to EXisting nodes:
        # "MATCH (ex0), (ex1)"

        # EXAMPLE of q_MERGE:
        '''
        MERGE (n)<-[:`EMPLOYS` ]-(ex0)
        MERGE (n)-[:`OWNS` {since: $EDGE1_par_1}]->(ex1)
        '''

        # EXAMPLE of q_WHERE:
        # "WHERE id(ex0) = 123 AND id(ex1) = 456"

        return q_MATCH, q_WHERE, q_MERGE, data_binding



    def create_node_with_relationships(self, labels, properties=None, connections=None) -> int:
        """
        Creates a new node with relationships to zero or more pre-existing nodes, identified by their labels and key/value pairs.

        Args:
            labels (str or list of str): A string, or list of strings, with label(s) to assign to the new node.
            properties (dict, optional): A dictionary of properties to assign to the new node. Defaults to None.
            connections (list of dict, optional): A (possibly empty) list of dictionaries with the following keys (all optional unless otherwise specified):
                "labels" (str or list of str): Labels of the existing node. Recommended.
                "key" (str): Key to identify the existing node. Required.
                "value" (str): Value to identify the existing node. Required.
                "rel_name" (str): The name to give to the new relationship. Required.
                "rel_dir" (str, optional): Either "OUT" or "IN", relative to the new node. Defaults to "OUT".
                "rel_attrs" (dict, optional): A dictionary of relationship attributes.

        Returns:
            int: If successful, an integer with the Neo4j internal ID of the node just created; otherwise, an Exception is raised.

        Raises:
            Exception: If the specified pre-existing nodes aren't found, then no new node is created, and an Exception is raised.

        Example:
            node_id = neptune_access.create_node_with_relationships(
                labels="PERSON",
                properties={"name": "Julian", "city": "Berkeley"},
                connections=[
                    {"labels": "DEPARTMENT", "key": "dept_name", "value": "IT", "rel_name": "EMPLOYS", "rel_dir": "IN"},
                    {"labels": ["CAR", "INVENTORY"], "key": "vehicle_id", "value": 12345, "rel_name": "OWNS", "rel_attrs": {"since": 2021}}
                ]
            )
        """

        # Prepare strings suitable for inclusion in a Cypher query, to define the new node to be created
        labels_str = CypherUtils.prepare_labels(labels)    # EXAMPLE:  ":`CAR`:`INVENTORY`"
        (cypher_props_str, data_binding) = CypherUtils.dict_to_cypher(properties)
        # EXAMPLE:
        #   cypher_props_str = "{`name`: $par_1, `city`: $par_2}"
        #   data_binding = {'par_1': 'Julian', 'par_2': 'Berkeley'}

        # Define the portion of the Cypher query to create the new node
        q_CREATE = f"CREATE (n {labels_str} {cypher_props_str})"
        # EXAMPLE:  "CREATE (n :`PERSON` {`name`: $par_1, `city`: $par_2})"
        #print("\n", q_CREATE)

        # Define the portion of the Cypher query to look up the requested existing nodes
        if connections is None or connections == []:
            q_MATCH = ""        # There will no need for a match, if there are no connections to be made
        else:
            q_MATCH = "MATCH"

        # Define the portion of the Cypher query to link up the new node to any of the existing ones
        q_MERGE = ""

        if properties is None:
            properties = []

        number_props_to_set = len(properties)   # Start building the total number of properties to set on the new node and on the relationships
        # (used to verify that the query ran as expected)

        for i, conn in enumerate(connections):
            #print(f"    i: {i}, conn: {conn}")
            match_labels = conn.get("labels")
            match_labels_str = CypherUtils.prepare_labels(match_labels)
            match_key = conn.get("key")
            if not match_key:
                raise Exception("Missing key name for the node to link to")
            match_value = conn.get("value")
            if not match_value:
                raise Exception("Missing key value for the node to link to")

            node_dummy_name = f"ex{i}"  # EXAMPLE: "ex3".   The "ex" stands for "existing node"
            data_binding_dummy = f"NODE{i}_VAL"
            q_MATCH += f" (ex{i} {match_labels_str} {{ {match_key}: ${data_binding_dummy} }})"
            # EXAMPLE of the incremental strings contributing to q_MATCH:
            #       "(ex0 :`DEPARTMENT` { dept_name: $NODE_0_VAL })"

            if i+1 < len(connections):
                q_MATCH += ","          # Comma separator, except at the end

            data_binding[data_binding_dummy] = match_value

            rel_name = conn.get("rel_name")
            if not rel_name:
                raise Exception("Missing name for the new relationship")

            rel_dir = conn.get("rel_dir", "OUT")        # "OUT" is the default value
            rel_attrs = conn.get("rel_attrs", None)     # By default, no relationship attributes

            #print("        rel_attrs: ", rel_attrs)
            (rel_attrs_str, cypher_dict_for_node) = CypherUtils.dict_to_cypher(rel_attrs, prefix=f"NODE{i}_")  # Process the optional relationship properties
            #print(f"rel_attrs_str: `{rel_attrs_str}` | cypher_dict_for_node: {cypher_dict_for_node}")
            # EXAMPLE of rel_attrs_str:        '{since: $NODE1_par_1}'  (possibly a blank string)
            # EXAMPLE of cypher_dict_for_node: {'NODE1_par_1': 2021}    (possibly an empty dict)

            data_binding.update(cypher_dict_for_node)           # Merge cypher_dict_for_node into the data_binding dictionary
            number_props_to_set += len(cypher_dict_for_node)    # This will be part of the summary count returned by Neo4j

            if rel_dir == "OUT":
                q_MERGE += f"MERGE (n)-[:{rel_name} {rel_attrs_str}]->({node_dummy_name})\n"  # Form an OUT-bound connection
                # EXAMPLE of term:  "MERGE (n)-[:OWNS {since: $NODE1_par_1}]->(ex1)"
            else:
                q_MERGE += f"MERGE (n)<-[:{rel_name} {rel_attrs_str}]-({node_dummy_name})\n"  # Form an IN-bound connection
                # EXAMPLE of term:  "MERGE (n)<-[:EMPLOYS ]-(ex0)"

        #print("q_MERGE:\n", q_MERGE)
        # EXAMPLE of q_MERGE:
        '''(n)<-[:EMPLOYS ]-(ex0)
        (n)-[:OWNS {since: $NODE1_par_1}]->(ex1)'''

        # Put all the parts of the Cypher query together
        q = q_MATCH + "\n" + q_CREATE + "\n" + q_MERGE + "RETURN id(n) AS internal_id"
        #print("\n", q)
        #print("\n", data_binding)
        # EXAMPLE of q:
        '''MATCH (ex0 :`DEPARTMENT` { dept_name: $NODE0_VAL }), (ex1 :`CAR`:`INVENTORY` { vehicle_id: $NODE1_VAL })
        CREATE (n :`PERSON` {`name`: $par_1, `city`: $par_2})
        MERGE (n)<-[:EMPLOYS ]-(ex0)
        MERGE (n)-[:OWNS {`since`: $NODE1_par_1}]->(ex1)
        RETURN id(n) AS internal_id
        '''
        # EXAMPLE of data_binding : {'par_1': 'Julian', 'par_2': 'Berkeley', 'NODE0_VAL': 'IT', 'NODE1_VAL': 12345, 'NODE1_par_1': 2021}

        result = self.update_query(q, data_binding)

        returned_data = result.get("returned_data")
        if len(returned_data) == 0:
            raise Exception("Unable to extract internal ID of the newly-created node")

        internal_id = returned_data[0].get("internal_id", None)
        if internal_id is None:    # Note: internal_id might be zero
            raise Exception("Unable to extract internal ID of the newly-created node")

        return internal_id    # Return the Neo4j ID of the new node



    #####################################################################################################

    '''                                      ~   DELETE NODES   ~                                     '''

    def ________DELETE_NODES________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################


    def delete_nodes_by_ids(self, internal_id: str) -> int:
        """
        Deletes the node or nodes specified by the internal_id argument from the database.

        Args:
            internal_id (str): The internal database ID of the node or nodes to delete.

        Returns:
            int: The number of nodes deleted (possibly zero).

        Warning:
            This function will delete data in the database. Use with caution.

        Example:
            num_deleted = neptune_access.delete_nodes_by_ids(internal_id='123')
        """
        # Create the "processed-match dictionaries"
        q = f"MATCH (n) WHERE id(n)='{internal_id}' WITH n DELETE n RETURN COUNT(n) AS deleted_count"

        
        print("q --- ",q)
        result = self.query(q)
        print("result - ",result)
        return result[0].get("deleted_count", 0)



    def delete_nodes_by_label(self, delete_labels=None, keep_labels=None) -> None:
        """
        Deletes nodes from the Neo4j database based on their labels.

        This function deletes nodes with the specified labels (`delete_labels`) from the database. If `keep_labels` is specified, nodes with these labels will be preserved, even if they also have labels in `delete_labels`. The `keep_labels` list has higher priority; if a label occurs in both lists, it will be kept. Note that this function does not clear indexes, so "ghost" labels may remain.

        Args:
            delete_labels (str or list of str, optional): A string or list of strings indicating specific labels to delete. Defaults to None.
            keep_labels (str or list of str, optional): A string or list of strings indicating specific labels to keep. Defaults to None.

        Returns:
            None

        Todo:
            - Return the number of nodes deleted.

        Warning:
            This function will delete data in the database. Use with caution.

        Example:
            neptune_access.delete_nodes_by_label(delete_labels=['User', 'Product'], keep_labels=['Order'])
        """
        if (delete_labels is None) and (keep_labels is None):
            # Delete ALL nodes AND ALL relationship from the database; for efficiency, do it all at once

            q = "MATCH (n) DETACH DELETE(n)"
            self.query(q)       # TODO: switch to update_query() and return the number of nodes deleted
            return

        if not delete_labels:
            delete_labels = self.get_labels()   # If no specific labels to delete were given,
            # then consider all labels for possible deletion (unless marked as "keep", below)
        else:
            if type(delete_labels) == str:
                delete_labels = [delete_labels] # If a string was passed, turn it into a list

        if not keep_labels:
            keep_labels = []    # Initialize list of labels to keep, if not provided
        else:
            if type(keep_labels) == str:
                keep_labels = [keep_labels] # If a string was passed, turn it into a list

        # Delete all nodes with labels in the delete_labels list,
        #   EXCEPT for any label in the keep_labels list
        for label in delete_labels:
            if not (label in keep_labels):
                q = f"MATCH (x:`{label}`) DETACH DELETE x"
                self.query(q)       # TODO: switch to update_query() and return the number of nodes deleted



    def empty_dbase(self, keep_labels=None, drop_indexes=False, drop_constraints=False) -> None:
        """
        Empties the database, optionally preserving nodes with certain labels, indexes, and constraints.

        This function deletes all nodes and relationships in the database, and optionally also all indexes and constraints. It can be instructed to preserve nodes with certain labels, as well as to preserve indexes and constraints.

        Args:
            keep_labels (list of str, optional): A list of labels to preserve. Nodes with these labels will not be deleted. Defaults to None.
            drop_indexes (bool, optional): Whether to delete all indexes. If False, indexes will be preserved. Defaults to False.
            drop_constraints (bool, optional): Whether to delete all constraints. If False, constraints will be preserved. Defaults to False.

        Returns:
            None

        Warning:
            This function will delete all data in the database. Use with caution.

        Example:
            neptune_access.empty_database(keep_labels=['User', 'Product'], drop_indexes=True, drop_constraints=True)
        """
        self.delete_nodes_by_label(keep_labels=keep_labels)

        if drop_indexes:
            self.drop_all_indexes(including_constraints=drop_constraints)




    #####################################################################################################

    '''                                      ~   MODIFY FIELDS   ~                                          '''

    def ________MODIFY_FIELDS________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################


    def set_fields(self, match: Union[int, NodeSpecs], set_dict: dict ) -> int:
        """
        Sets the specified fields of the given node(s) to the specified values.

        This function locates the node or nodes specified by `match`, and sets each field specified in `set_dict` to the corresponding value. If a field does not exist in a node, it is created. If it does exist, its value is updated. Other fields are left undisturbed.

        Args:
            match (Union[int, NodeSpecs]): Either an integer with an internal database node id, or a "NodeSpecs" object, as returned by match(), with data to identify a node or set of nodes.
            set_dict (dict): A dictionary of field name/values to create/update the node's attributes. Note: blanks ARE allowed in the keys.

        Returns:
            int: The number of properties set.

        Todo:
            - If any field is blank, offer the option to drop it altogether from the node, with a "REMOVE n.field" statement in Cypher; doing SET n.field = "" doesn't drop it.

        Example:
            match_structure = match(labels = "car", properties = {"vehicle id": 123})
            set_fields(match=match_structure, set_dict = {"color": "white", "price": 7000})
        """

        if set_dict == {}:
            return 0             # There's nothing to do

        match_structure = CypherUtils.process_match_structure(match, caller_method="set_fields")

        if self.debug:
            print("In set_fields()")
            print("    match_structure:", match_structure)

        # Unpack needed values from the match dictionary
        (node, where, data_binding, dummy_node_name) = match_structure.unpack_match()

        cypher_match = f"MATCH {node} {CypherUtils.prepare_where(where)} "

        set_list = []
        for field_name, field_value in set_dict.items():        # field_name, field_value are key/values in set_dict
            field_name_safe = field_name.replace(" ", "_")      # To protect against blanks in name.  E.g., "end date" becomes "end_date"
            set_list.append(f"{dummy_node_name}.`{field_name}` = ${field_name_safe}")    # Example:  "n.`field1` = $field1"
            data_binding[field_name_safe] = field_value                                  # EXTEND the Cypher data-binding dictionary

        # Example of data_binding at the end of the loop: {'n_par_1': 123, 'n_par_2': 7500, 'color': 'white', 'price': 7000}
        #       in this example, the first 2 keys arise from the match (find) operation to locate the node,
        #       while the last 2 are for the use of the SET operation


        # Note: set_list cannot be empty, because we eliminated the scenario set_dict == {} at the beginning
        set_clause = "SET " + ", ".join(set_list)   # Example:  "SET n.`color` = $color, n.`price` = $price"

        cypher = cypher_match + set_clause

        # Example of cypher:
        # "MATCH (n :`car` {`vehicle id`: $n_par_1, `price`: $n_par_2})  SET n.`color` = $color, n.`price` = $price"
        # Example of data binding:
        #       {'n_par_1': 123, 'n_par_2': 7500, 'color': 'white', 'price': 7000}

        stats = self.update_query(cypher, data_binding)

        number_properties_set = stats.get("properties_set", 0)
        return number_properties_set




    #####################################################################################################

    '''                                    ~   RELATIONSHIPS   ~                                      '''

    def ________RELATIONSHIPS________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################


    # def get_relationship_types(self) -> [str]:
    # todo:call yield queries not supported by Neptune
    #     """
    #     Extract and return a list of all the Neo4j relationship names (i.e. types of relationships)
    #     present in the entire database, in no particular order

    #     :return:    A list of strings
    #     """
    #     results = self.query("call db.relationshipTypes() yield relationshipType return relationshipType")
    #     return [x['relationshipType'] for x in results]



    def add_links(self, from_id: str, to_id: str, rel_name:str) -> dict:
        """
        Adds a link (also known as a graph edge or relationship) between two nodes in the graph database.

        This function creates a relationship with the specified name (`rel_name`), originating from the node specified by `from_id` and terminating at the node specified by `to_id`. If a relationship with the same name already exists between the same nodes, no new relationship is created and an exception is raised.

        Args:
            from_id (str): The internal database ID of the origin node.
            to_id (str): The internal database ID of the destination node.
            rel_name (str): The name to give to the new relationship. Blanks are allowed.

        Returns:
            dict: A dictionary containing the number of edges added.

        Raises:
            Exception: If no edges were added, or in case of an error.

        Todo:
            Add a `rel_props` argument to specify properties for the relationship. It's currently unclear what would happen if multiple calls were made with this argument: would it update the properties or create a new relationship?
        """
        print(from_id,to_id)
        
        # Prepare the query to add the requested links between the given nodes (possibly, sets of nodes)
        q = f'''
            MATCH (from),(to)
            WHERE id(from) = '{from_id}' AND id(to) = '{to_id}'
            MERGE (from) -[:{rel_name}]-> (to)   
            '''

        print("q  -- ",q)
        self.query(q)


        q=f"""MATCH (from)-[:{rel_name}]->(to)
            WHERE id(from) = '{from_id}' AND id(to) = '{to_id}'
            RETURN COUNT(*) AS link_count"""

        print("q --- ",q)

        result=self.query(q)
        print("result",result)
        return result
    


    # TODO: add a method to remove all links of a given name emanating to or from a given node
    #       - as done for Schema.remove_all_data_relationship()
    def remove_links(self, from_id: str, to_id: str, rel_name: str) -> dict:
        """
        Removes one or more links (also known as relationships or edges) between two nodes in the graph database.

        This function removes relationships with the specified name (`rel_name`), originating from the node specified by `from_id` and terminating at the node specified by `to_id`. If `rel_name` is None or a blank string, all relationships between the two nodes are removed.

        Args:
            from_id (str): The internal database ID of the origin node.
            to_id (str): The internal database ID of the destination node.
            rel_name (str, optional): The name of the relationship to delete between the two specified nodes. If None or a blank string, all relationships between those two nodes will be deleted. Defaults to None.

        Returns:
            dict: A dictionary containing the number of edges removed.

        Raises:
            Exception: If no edges were removed, or in case of an error.

        Notes:
            - The nodes themselves are left untouched.
            - More than one node could be present in either of the matches.
            - The number of relationships deleted could be more than one even with a single "from" node and a single "to" node. Neo4j allows multiple relationships with the same name between the same two nodes, as long as the relationships differ in their properties.
        """
        cypher_query = (
            f"MATCH (from)-[r:{rel_name}]->(to) "
            f"WHERE id(from) = '{from_id}' AND id(to) = '{to_id}' "
            "DELETE r RETURN count(r) AS deleted_count"
        )
        result = self.query(cypher_query)
        return result



    def links_exist(self, from_id: str, to_id: str, rel_name: str) -> bool:
        """
        Checks if one or more relationships with the specified name exist between the specified nodes.

        This function checks for the existence of relationships with the specified name (`rel_name`), originating from the node specified by `from_id` and terminating at the node specified by `to_id`. The function returns True if one or more such relationships exist, and False otherwise.

        Args:
            from_id (str): The internal database ID of the origin node, or a NodeSpecs object identifying a node or set of nodes.
            to_id (str): The internal database ID of the destination node, or a NodeSpecs object identifying a node or set of nodes.
            rel_name (str): The name of the relationship to check for between the two specified nodes. Blanks are allowed.

        Returns:
            bool: True if one or more relationships with the specified name exist between the specified nodes, False otherwise.

        Notes:
            - The `from_id` and `to_id` arguments can be either an integer with an internal database node id, or a "NodeSpecs" object, as returned by match(), with data to identify a node or set of nodes.
            - If `from_id` and `to_id` are created by calls to match(), in scenarios where a clause dummy name is actually used, they MUST use different clause dummy names.
        """
        return self.number_of_links(from_id, to_id, rel_name=rel_name) >= 1   # True if at least 1



    def number_of_links(self, from_id: str, to_id: str, rel_name: str) -> int:
        """
        Returns the number of relationships with the given name that exist between the specified nodes.

        This function counts the number of relationships with the specified name (`rel_name`), originating from the node or nodes specified by `from_id` and terminating at the node or nodes specified by `to_id`.

        Args:
            from_id (str): The internal database ID of the origin node, or a NodeSpecs object identifying a node or set of nodes.
            to_id (str): The internal database ID of the destination node, or a NodeSpecs object identifying a node or set of nodes. Note: `from_id` and `to_id`, if created by calls to match(), in scenarios where a clause dummy name is actually used, MUST use different clause dummy names.
            rel_name (str): The name of the relationship to look for between the specified nodes or groups of nodes. Blanks are allowed.

        Returns:
            int: The number of relationships that were found.

        """
        q=f"""MATCH (from)-[:{rel_name}]->(to)
            WHERE id(from) = '{from_id}' AND id(to) = '{to_id}'
            RETURN COUNT(*) AS link_count"""

        print("q --- ",q)

        result=self.query(q)
        print("result",result)
        return result[0].get("link_count", 0)



    def reattach_node(self, node: str, old_attachment: str, new_attachment: str, rel_name: str, rel_name_new: str=None) -> None:
        """
        Severs the relationship with the given name from the given node to the node `old_attachment`,
        and re-creates it to the node `new_attachment` (optionally under a different relationship name).

        Note: relationship properties, if present, will NOT be transferred.

        Args:
            node (str): The internal database ID of the node to detach and reattach.
            old_attachment (str): The internal database ID of the other node currently connected to `node`.
            new_attachment (str): The internal database ID of the new node to connect to `node`.
            rel_name (str): Name of the old relationship name.
            rel_name_new (str, optional): Name of the new relationship name. Defaults to the same as `rel_name`.

        Returns:
            None. If unsuccessful, an Exception is raised.

        Notes:
            - This function uses a Cypher `MATCH` statement to find the nodes and relationship to modify, a `MERGE` statement to create the new relationship, and a `DELETE` statement to remove the old relationship.
            - The function prints the Cypher query and the result of executing the query.
        """

        if rel_name_new is None:
            rel_name_new = rel_name     # Use the default value, if not provided

        q = f'''
            MATCH (node_start) -[rel :{rel_name}]-> (node_old), (node_new)
            WHERE id(node_start) = '{node}' and id(node_old) = '{old_attachment}' and id(node_new) = '{new_attachment}'
            MERGE (node_start) -[:{rel_name_new}]-> (node_new)
            DELETE rel         
            '''
        result = self.query(q)
        
    def link_nodes_by_ids(self, node_id1:str, node_id2:str, rel:str, rel_props = None) -> None:
        """
        Locates two Neo4j nodes with the given Neo4j internal IDs and adds a relationship between them.

        This function finds the nodes with the specified IDs (`node_id1` and `node_id2`), and if both nodes are found, it adds a relationship with the specified name (`rel`) from the first node to the second node. If a relationship with the same name already exists between the same nodes, no new relationship is created.

        Args:
            node_id1 (str): The internal database ID of the first node.
            node_id2 (str): The internal database ID of the second node.
            rel (str): The name to give to the new relationship. Blanks are allowed.
            rel_props (dict, optional): A dictionary representing the properties of the new relationship. The keys should be property names and the values should be the corresponding property values. Defaults to None.

        Returns:
            None

        Example:
            link_nodes_by_ids(123, 88, "AVAILABLE_FROM", {'cost': 1000})
        """
        cypher_rel_props, cypher_dict = CypherUtils.dict_to_cypher(rel_props)  # Process the optional relationship properties

        q = f"""
        MATCH (x), (y) 
        WHERE id(x) = '{node_id1}' AND id(y) = '{node_id2}'
        MERGE (x)-[r:`{rel}`]->(y)
        RETURN count(r) as rel_count
        """
        # Extend the (possibly empty) Cypher data dictionary, to also include a value for the key "node_id1" and "node_id2"
        cypher_dict["node_id1"] = node_id1
        cypher_dict["node_id2"] = node_id2

        self.query(q, cypher_dict)



    def link_nodes_on_matching_property(self, label1:str, label2:str, property1:str, rel:str, property2=None) -> None:
        """
        Locates pairs of Neo4j nodes with specified labels and matching properties, and adds a relationship between them.

        This function finds pairs of nodes where the first node has the label `label1`, the second node has the label `label2`, and the two nodes have the same value for `property1` (or `property1` in the first node and `property2` in the second node, if `property2` is specified). For each such pair, it adds a relationship with the specified name (`rel`) from the first node to the second node, unless such a relationship already exists.

        Args:
            label1 (str): The label that the first node must have.
            label2 (str): The label that the second node must have.
            property1 (str): The property that must be present in the first node (and also in the second node, if `property2` is None).
            rel (str): The name to give to the new relationships.
            property2 (str, optional): The property that must be present in the second node, if different from `property1`. Defaults to None.

        Returns:
            None

        Notes:
            - This operation is akin to a "JOIN" in a relational database.
            - The function uses a Cypher `MATCH` statement to find the nodes and a `MERGE` statement to create the relationships.
        """
        if not property2:
            property2 = property1

        q = f'''MATCH (x:`{label1}`), (y:`{label2}`) WHERE x.`{property1}` = y.`{property2}` 
                MERGE (x)-[:{rel}]->(y)'''

        self.query(q)





    #####################################################################################################

    '''                                    ~   FOLLOW LINKS   ~                                        '''

    def ________FOLLOW_LINKS________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################


    def follow_links(self, match: Union[int, NodeSpecs], rel_name: str, rel_dir ="OUT", neighbor_labels = None) -> [dict]:
        """
        Follows all the relationships of the given name to or from the given starting node(s), and returns all the properties of the neighbor nodes.

        This function starts from the node or nodes specified by `match`, and follows all relationships with the specified name (`rel_name`) in the specified direction (`rel_dir`). It then returns a list of dictionaries containing all the properties of the neighbor nodes that are reached by following the relationships. Optionally, it can filter the neighbor nodes based on their labels (`neighbor_labels`).

        Args:
            match (Union[int, NodeSpecs]): Either an integer with an internal database node id, or a "NodeSpecs" object, as returned by match(), with data to identify a node or set of nodes.
            rel_name (str): The name of the relationship to follow. Note: any other relationships are ignored.
            rel_dir (str, optional): Either "OUT"(default), "IN" or "BOTH". Direction(s) of the relationship to follow.
            neighbor_labels (Union[str, List[str]], optional): Optional label(s) required on the neighbors. If present, either a string or list of strings.

        Returns:
            List[dict]: A list of dictionaries with all the properties of the neighbor nodes.

        Todo:
            - Add a method that fetches the ID's of those nodes, rather than their properties.
            - Maybe add the option to just return a subset of fields.
        """
        match_structure = CypherUtils.process_match_structure(match, caller_method="follow_links")

        if self.debug:
            print("In follow_links()")
            print("    match_structure:", match_structure)

        # Unpack needed values from the match dictionary
        (node, where, data_binding, _) = match_structure.unpack_match()

        neighbor_labels_str = CypherUtils.prepare_labels(neighbor_labels)     # EXAMPLE:  ":`CAR`:`INVENTORY`"

        if rel_dir == "OUT":    # Follow outbound links
            q =  f"MATCH {node} - [:{rel_name}] -> (neighbor {neighbor_labels_str})"
        elif rel_dir == "IN":   # Follow inbound links
            q =  f"MATCH {node} <- [:{rel_name}] - (neighbor {neighbor_labels_str})"
        else:                   # Follow links in BOTH directions
            q =  f"MATCH {node}  - [:{rel_name}] - (neighbor {neighbor_labels_str})"


        q += CypherUtils.prepare_where(where) + " RETURN neighbor"

        result = self.query(q, data_binding, single_column='neighbor')

        return result



    def count_links(self, match: Union[int, NodeSpecs], rel_name: str, rel_dir="OUT", neighbor_labels = None) -> int:
        """
        Counts all the relationships of the given name to and/or from the given starting node(s), into/from neighbor nodes (optionally having the given labels).

        This function starts from the node or nodes specified by `match`, and counts all relationships with the specified name (`rel_name`) in the specified direction (`rel_dir`). Optionally, it can filter the neighbor nodes based on their labels (`neighbor_labels`).

        Args:
            match (Union[int, NodeSpecs]): Either an integer with an internal database node id, or a "NodeSpecs" object, as returned by match(), with data to identify a node or set of nodes.
            rel_name (str): The name of the relationship to count. Note: any other relationships are ignored.
            rel_dir (str, optional): Either "OUT"(default), "IN" or "BOTH". Direction(s) of the relationship to count.
            neighbor_labels (Union[str, List[str]], optional): Optional label(s) required on the neighbors. If present, either a string or list of strings.

        Returns:
            int: The total number of inbound and/or outbound relationships to the given node(s).
        """
        match_structure = CypherUtils.process_match_structure(match, caller_method="count_links")

        if self.debug:
            print("In count_links()")
            print("    match_structure:", match_structure)

        # Unpack needed values from the match dictionary
        (node, where, data_binding, _) = match_structure.unpack_match()

        neighbor_labels_str = CypherUtils.prepare_labels(neighbor_labels)     # EXAMPLE:  ":`CAR`:`INVENTORY`"

        if rel_dir == "OUT":            # Follow outbound links
            q =  f"MATCH {node} - [:{rel_name}] -> (neighbor {neighbor_labels_str})"
        elif rel_dir == "IN":           # Follow inbound links
            q =  f"MATCH {node} <- [:{rel_name}] - (neighbor {neighbor_labels_str})"
        elif rel_dir == "BOTH":         # Follow links in BOTH directions
            q =  f"MATCH {node}  - [:{rel_name}] - (neighbor {neighbor_labels_str})"
        else:
            raise Exception(f"count_links(): argument `rel_dir` must be one of: 'IN', 'OUT', 'BOTH'; value passed was `{rel_dir}`")

        q += CypherUtils.prepare_where(where) + " RETURN count(neighbor) AS link_count"

        return self.query(q, data_binding, single_cell="link_count")



    def get_parents_and_children(self, internal_id: str) -> Dict[str, List[Dict[str, Union[int, str]]]]:
        """
        Fetches all the nodes connected to the given node by inbound relationships (its "parents") and outbound relationships (its "children").

        This function finds the node with the specified internal ID (`internal_id`), and returns a dictionary containing two lists: one with all the nodes connected to the given node by inbound relationships (its "parents"), and one with all the nodes connected to the given node by outbound relationships (its "children"). Each node is represented as a dictionary with the keys "internal_id", "labels", and "rel".

        Args:
            internal_id (str): The internal database ID of the node to find.

        Returns:
            Dict[str, List[Dict[str, Union[int, str]]]]: A dictionary with two keys: 'parent_list' and 'child_list'. Each value is a list of dictionaries, each representing a node connected to the given node by an inbound or outbound relationship, respectively.

        Todo:
            - Allow specifying a relationship name to follow.

        Example:
            {'parent_list': [{'internal_id': 163, 'labels': ['Subject'], 'rel': 'HAS_TREATMENT'}], 'child_list': []}
        """

        # Fetch the parents
        cypher = f"MATCH (parent)-[inbound]->(n) WHERE id(n) = '{internal_id}' " \
                 "RETURN id(parent) AS internal_id, labels(parent) AS labels, type(inbound) AS rel"

        parent_list = self.query(cypher)

        # Fetch the children
        cypher = f"MATCH (n)-[outbound]->(child) WHERE id(n) = '{internal_id}' " \
                 "RETURN id(child) AS internal_id, labels(child) AS labels, type(outbound) AS rel"

        child_list = self.query(cypher)

        return (parent_list, child_list)



    def get_siblings(self, internal_id: str, rel_name: str, rel_dir="OUT", order_by=None) -> [int]:
        """
        Returns the data of all the "sibling" nodes of the given node.

        By "sibling", we mean: "sharing a link (by default outbound) of the specified name, to a common other node". For example, two nodes, "French" and "German", each with an outbound link named "subcategory_of" to a third node, will be considered "siblings" under rel_name="subcategory_of" and rel_dir="OUT".

        Args:
            internal_id (str): The internal database ID of the node of interest.
            rel_name (str): The name of the relationship used to establish a "siblings" connection.
            rel_dir (str, optional): Either "OUT" (default) or "IN". The link direction that is expected from the start node to its "parents" - and then IN REVERSE to the parent's children.
            order_by (str, optional): If specified, it must be the name of a field in the sibling nodes, to order the results by.

        Returns:
            List[dict]: A list of dictionaries, with one element for each "sibling"; each element contains the 'internal_id' and 'neo4j_labels' keys, plus whatever attributes are stored on that node.

        Todo:
            - Allow specifying a relationship name to follow.
            - Test the 'order_by' parameter.

        Example:
            [{'name': 'French', 'internal_id': 123, 'neo4j_labels': ['Categories']}]
        """
        # CypherUtils.assert_valid_internal_id(internal_id)

        assert type(rel_name) == str, \
            f"get_siblings(): argument `rel_name` must be a string; " \
            f"the given value ({rel_name}) is of type {type(rel_name)}"

        # Follow the links with the specified name, in the indicated direction from the given link,
        # and then in the reverse direction
        if rel_dir == "OUT":
            q = f"""
                MATCH (n) - [:{rel_name}] -> (parent) <- [:{rel_name}] - (sibling)
                WHERE id(n) = '{internal_id}'
                RETURN sibling
                """
            print("q --- ",q)
            
        elif rel_dir == "IN":
            q = f"""
                MATCH (n) <- [:{rel_name}] - (parent) - [:{rel_name}] -> (sibling)
                WHERE id(n) = '{internal_id}'
                RETURN sibling
                """
            print("q --- ",q)            
        else:
            raise Exception(f"get_siblings(): unknown value for the `rel_dir` argument ({rel_dir}); "
                            f"allowed values are 'IN' and 'OUT'")

        if order_by:
            q += f'''
                ORDER BY toLower(sibling.{order_by}])
                '''

        print("final_query - ",q)
        result = self.query_extended(q, data_binding={"internal_id": internal_id}, flatten=True)

        print("result - ",result)
        return result





    #####################################################################################################

    '''                                      ~   LABELS   ~                                           '''

    def ________LABELS________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################


    def get_labels(self) -> [str]:
        """
        Extracts and returns a list of all the Neo4j labels present in the database.

        This function retrieves all unique labels from the nodes in the database. The order of the labels in the returned list is not guaranteed. To get the labels of a specific node, use the `get_node_labels()` function.

        Returns:
            list of str: A list of all unique labels present in the database.

        Todo:
            - Test when there are nodes that have multiple labels.

        Example:
            labels = neptune_access.get_labels()
        """
        # results = self.query("call db.labels() yield label return label")
        results = self.query("MATCH (n) RETURN DISTINCT labels(n) AS label ")

        print("results - ",results)

        labels = [label for result in results for label in result['label']]

        return labels


    # def get_label_properties(self, label:str) -> list:
    # todo:no need of this anymore:
    #     """
    #     Extract and return all the property (key) names used in nodes with the given label,
    #     sorted alphabetically

    #     :param label:   A string with the name of a node label
    #     :return:        A list of property names, sorted alphabetically
    #     """
    #     q = """
    #         CALL db.schema.nodeTypeProperties() 
    #         YIELD nodeLabels, propertyName
    #         WHERE $label in nodeLabels and propertyName IS NOT NULL
    #         RETURN DISTINCT propertyName 
    #         ORDER BY propertyName
    #         """
    #     data_binding = {'label': label}

    #     return [res['propertyName'] for res in self.query(q, data_binding)]

    def get_label_properties(self, label: str) -> list:
        """
        Extracts and returns all the property (key) names used in nodes with the given label, sorted alphabetically.

        This function retrieves all unique property names from the nodes in the database that have the specified label. The property names are returned in alphabetical order.

        Args:
            label (str): A string with the name of a node label.

        Returns:
            list: A list of property names, sorted alphabetically.

        Example:
            properties = neptune_access.get_label_properties(label='User')
        """

#         todo: swapnil:
#         the error is that the Amazon Neptune service, which is compatible with OpenCypher, may not support certain Neo4j-specific procedures like db.     schema.nodeTypeProperties(). This procedure is specific to Neo4j and may not be available in Amazon Neptune.To retrieve property names of nodes with a given label in Amazon Neptune, you can modify your approach. Instead of using Neo4j-specific procedures, you can directly query the nodes and extract their properties dynamically.
        q = f"MATCH (n:{label}) RETURN DISTINCT keys(n) AS properties"
        result = self.query(q)
        return sorted(list(set(property for res in result for property in res['properties'])))



    #####################################################################################################

    '''                                      ~   INDEXES   ~                                          '''

    def ________INDEXES________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################


    def get_indexes(self) -> pd.DataFrame:
        """
        Return all the database indexes, and some of their attributes,
        as a Pandas dataframe.

        EXAMPLE:
               labelsOrTypes              name          properties    type  uniqueness
             0    ["my_label"] "index_23b59623"    ["my_property"]   BTREE   NONUNIQUE
             1    ["L"]          "L.client_id"       ["client_id"]   BTREE      UNIQUE

        :return:        A (possibly-empty) Pandas dataframe
        """

        q = f"""
          CALL db.indexes() 
          YIELD name, labelsOrTypes, properties, type, uniqueness
          return *
          """

        results = self.query(q)
        if len(results) > 0:
            return pd.DataFrame(list(results))
        else:
            return pd.DataFrame([], columns=['name'])



    def create_index(self, label :str, key :str) -> bool:
        """
        Create a new database index, unless it already exists,
        to be applied to the specified label and key (property).
        The standard name given to the new index is of the form label.key
        EXAMPLE - to index nodes labeled "car" by their key "color", use:
                        create_index("car", "color")
                  This new index - if not already in existence - will be named "car.color"
        If an existing index entry contains a list of labels (or types) such as ["l1", "l2"] ,
        and a list of properties such as ["p1", "p2"] ,
        then the given pair (label, key) is checked against ("l1_l2", "p1_p2"), to decide whether it already exists.

        :param label:   A string with the node label to which the index is to be applied
        :param key:     A string with the key (property) name to which the index is to be applied
        :return:        True if a new index was created, or False otherwise
        """
        existing_indexes = self.get_indexes()   # A Pandas dataframe with info about indexes;
                                                #       in particular 2 columns named "labelsOrTypes" and "properties"

        # Index is created if not already exists.
        # a standard name for the index is assigned: `{label}.{key}`
        existing_standard_name_pairs = list(existing_indexes.apply(
            lambda x: ("_".join(x['labelsOrTypes']), "_".join(x['properties'])), axis=1))   # Proceed by row
        """
        For example, if the Pandas dataframe existing_indexes contains the following columns: 
                            labelsOrTypes     properties
                0                   [car]  [color, make]
                1                [person]          [sex]
                
        then existing_standard_names will be:  [('car', 'color_make'), ('person', 'sex')]
        """

        if (label, key) not in existing_standard_name_pairs:
            q = f'CREATE INDEX `{label}.{key}` FOR (s:`{label}`) ON (s.`{key}`)'
            self.query(q)
            return True
        else:
            return False



    def drop_index(self, name: str) -> bool:
        """
        Get rid of the index with the given name

        :param name:    Name of the index to jettison
        :return:        True if successful or False otherwise (for example, if the index doesn't exist)
        """
        try:
            self.query(f"DROP INDEX `{name}`")      # Note: this crashes if the index doesn't exist
            return True
        except Exception:
            return False


    def drop_all_indexes(self, including_constraints=True) -> None:
        """
        Eliminate all the indexes in the database and, optionally, also get rid of all constraints

        :param including_constraints:   Flag indicating whether to also ditch all the constraints
        :return:                        None
        """
        if including_constraints:
            if self.apoc:
                self.query("call apoc.schema.assert({},{})")
            else:
                self.drop_all_constraints()    # TODO: it doesn't work in version 5.5 of the Neo4j database

        indexes = self.get_indexes()
        for name in indexes['name']:
            self.drop_index(name)




    #####################################################################################################

    '''                                     ~   CONSTRAINTS   ~                                        '''

    def ________CONSTRAINTS________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################


    def get_constraints(self) -> pd.DataFrame:    # TODO: it doesn't work in version 5.5 of the database
        """
        Return all the database constraints, and some of their attributes,
        as a Pandas dataframe with 3 columns:
            name        EXAMPLE: "my_constraint"
            description EXAMPLE: "CONSTRAINT ON ( patient:patient ) ASSERT (patient.patient_id) IS UNIQUE"
            details     EXAMPLE: "Constraint( id=3, name='my_constraint', type='UNIQUENESS',
                                  schema=(:patient {patient_id}), ownedIndex=12 )"
        :return:  A (possibly-empty) Pandas dataframe
        """
        q = """
           call db.constraints() 
           yield name, description, details
           return *
           """
        results = self.query(q)
        if len(results) > 0:
            return pd.DataFrame(list(results))
        else:
            return pd.DataFrame([], columns=['name'])



    def create_constraint(self, label: str, key: str, type="UNIQUE", name=None) -> bool:
        """
        Create a uniqueness constraint for a node property in the graph,
        unless a constraint with the standard name of the form `{label}.{key}.{type}` is already present
        Note: it also creates an index, and cannot be applied if an index already exists.
        EXAMPLE: create_constraint("patient", "patient_id")
        :param label:   A string with the node label to which the constraint is to be applied
        :param key:     A string with the key (property) name to which the constraint is to be applied
        :param type:    For now, the default "UNIQUE" is the only allowed option
        :param name:    Optional name to give to the new constraint; if not provided, a
                            standard name of the form `{label}.{key}.{type}` is used.  EXAMPLE: "patient.patient_id.UNIQUE"
        :return:        True if a new constraint was created, or False otherwise
        """
        assert type == "UNIQUE"
        #TODO: consider other types of constraints

        existing_constraints = self.get_constraints()
        # constraint is created if not already exists.
        # a standard name for a constraint is assigned: `{label}.{key}.{type}` if name was not provided
        cname = (name if name else f"`{label}.{key}.{type}`")
        if cname in list(existing_constraints['name']):
            return False

        try:
            q = f'CREATE CONSTRAINT {cname} ON (s:`{label}`) ASSERT s.`{key}` IS UNIQUE'
            self.query(q)
            # Note: creation of a constraint will crash if another constraint, or index, already exists
            #           for the specified label and key
            return True
        except Exception:
            return False



    def drop_constraint(self, name: str) -> bool:
        """
        Eliminate the constraint with the specified name.
        :param name:    Name of the constraint to eliminate
        :return:        True if successful or False otherwise (for example, if the constraint doesn't exist)
        """
        try:
            q = f"DROP CONSTRAINT `{name}`"
            self.query(q)     # Note: it crashes if the constraint doesn't exist
            return True
        except Exception:
            return False



    def drop_all_constraints(self) -> None:
        """
        Eliminate all the constraints in the database
        :return:    None
        """
        constraints = self.get_constraints()
        for name in constraints['name']:
            self.drop_constraint(name)




    #####################################################################################################

    '''                              ~   READ IN from PANDAS   ~                                      '''

    def ________READ_IN_PANDAS________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################

    def load_pandas(
            self,
            df :Union[pd.DataFrame, pd.Series], labels :Union[str, List[str], Tuple[str]],
            merge_primary_key=None, merge_overwrite=False,
            rename=None, ignore_nan=True, max_chunk_size=10000) -> [int]:
        """
        Load a Pandas Data Frame (or Series) into Neo4j.
        Each row is loaded as a separate node.

        Columns whose dtype is integer will appear as integer data types in the Neo4j nodes
            (however, if a NaN is present in a Pandas column, it'll automatically be transformed to float)

        Database indexes are added as needed, if the "merge_primary_key" argument is used.

        TODO: maybe save the Panda data frame's row number as an attribute of the Neo4j nodes, to ALWAYS have a primary key
              maybe allow a bulk auto-increment field
              maybe allow to link all nodes to a given existing one

        :param df:              A Pandas Data Frame (or Series) to import into Neo4j.
                                    If it's a Series, it's treated as a single column (named "value", if it lacks a name)

        :param labels:          A string, or list/tuple of strings - representing one or more Neo4j labels
                                    to use on all the newly-created nodes

        :param merge_primary_key: (OPTIONAL) Used to request that new records be merged (rather than added)
                                    if they already exist, as determined by the string with the name of the field
                                    that serves as a primary key.
                                    TODO: to allow for list of primary_keys
        :param merge_overwrite: (OPTIONAL) Only applicable if "merge_primary_key" is set.
                                    If True then on merge the existing nodes will be completely overwritten with the new data,
                                    otherwise they will be updated with new information (keys that are not present
                                    in the df argument will be left unaltered)

        :param rename:          Optional dictionary to rename the Pandas dataframe's columns to
                                    EXAMPLE {"current_name": "name_we_want"}
        :param ignore_nan       If True, node properties created from columns of dtype float
                                    will only be set if they are not NaN.
                                    (Note: the moment a NaN is present, columns of integers in a dataframe
                                           will automatically become floats)
        :param max_chunk_size:  To limit the number of rows loaded at one time

        :return:                A (possibly-empty) list of the internal database ID's of the created nodes
        """
        if isinstance(df, pd.Series):
            # Convert a Pandas Series into a Data Frame
            if df.name is None:
                df = pd.DataFrame(df, columns = ["value"])
            else:
                df = pd.DataFrame(df)


        if rename is not None:
            df = df.rename(rename, axis=1)  # Rename the columns in the Pandas data frame


        # Convert Pandas' datetime format to Neo4j's
        df = self.pd_datetime_to_neo4j_datetime(df)


        # Process the primary keys, if any
        primary_key_s = ''
        if merge_primary_key is not None:
            neo_indexes = self.get_indexes()
            if f"{labels}.{merge_primary_key}" not in list(neo_indexes['name']):
                # Create a new database index
                if type(labels) == str:
                    index_label = labels
                else:
                    index_label = labels[0]     # In case of multiple labels, take the first

                self.create_index(index_label, merge_primary_key)
                time.sleep(1)   # Used to give Neo4j time to populate the index

            primary_key_s = ' {' + f'`{merge_primary_key}`:record[\'{merge_primary_key}\']' + '}'
            # EXAMPLE of primary_key_s: "{patient_id:record['patient_id']}"
            # Note that "record" is a dummy name used in the Cypher query, further down


        numeric_columns = []
        # Numeric columns are handled differently if the ignore_nan flag is set
        if ignore_nan:
            for col, dtype in df.dtypes.items():        # Loop over all columns and their types
                #if dtype in ['float64', 'float32']:
                if pd.api.types.is_float_dtype(dtype):  # This will cover all floats
                                                        # Note: a column with a NaN is automatically a float even if all values are int
                    numeric_columns.append(col)

        if merge_primary_key in numeric_columns:
            assert not (df[merge_primary_key].isna().any()), \
                    f"Cannot merge nodes on NULL values in column `{merge_primary_key}`. " \
                    "Eliminate missing values"


        op = 'MERGE' if merge_primary_key else 'CREATE'   # A "MERGE" or "CREATE" operation, as needed
        if (not merge_primary_key) or merge_overwrite:
            set_operator = ""
        else:
            set_operator = "+"

        cypher_labels = CypherUtils.prepare_labels(labels)


        res = []                                                # Running list of the internal database ID's of the created nodes

        # Determine the number of needed batches (always at least 1)
        number_batches = math.ceil(len(df) / max_chunk_size)    # Note that if the max_chunk_size equals the size of the df
                                                                # then we'll just use 1 batch

        batch_list = np.array_split(df, number_batches)         # List of Pandas Data Frames

        for df_chunk in batch_list:         # Split the operation into batches
            # df_chunk is a Pandas Data Frame
            record_list = df_chunk.to_dict(orient='records')    # Example: [{'col1': 1, 'col2': 0.5}, {'col1': 2, 'col2': 0.75}]
                                                                # Note: PyCharm complains about to_dict()
                                                                #       because it fails to realize that df_chunk is a dataframe,
                                                                #       not an ndarray
            if numeric_columns:
                q = f'''
                    WITH $data AS data 
                    UNWIND data AS record 
                    WITH record, [key in $numeric_columns WHERE toString(record[key]) = 'NaN'] as exclude_keys
                    {op} (n {cypher_labels}{primary_key_s}) 
                    SET n {set_operator}= apoc.map.removeKeys(record, exclude_keys)
                    RETURN id(n) as node_id 
                    '''
                cypher_dict = {'data': record_list, 'numeric_columns': numeric_columns}
            else:
                q = f'''
                    WITH $data AS data 
                    UNWIND data AS record 
                    {op} (n {cypher_labels}{primary_key_s}) 
                    SET n {set_operator}= record 
                    RETURN id(n) as node_id 
                    '''
                cypher_dict = {'data':record_list}


            res_chunk = self.query(q, cypher_dict, single_column="node_id") # A (possibly empty) list of internal ID's
            res += res_chunk
        # END for loop

        return res



    def pd_datetime_to_neo4j_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        If any column in the given Pandas Data Frame is of dtype datetime or timedelta,
        replace its entries with a Neo4j-friendly datetime type.

        If any change is needed, return a modified COPY of the dataframe;
        otherwise, return the original dataframe (no cloning done)

        EXAMPLE: an entry such as pd.Timestamp('2023-01-01 00:00:00')
                 will become an object neo4j.time.DateTime(2023, 1, 1, 0, 0, 0, 0)

        :param df:  A Pandas data frame
        :return:    Either the same data frame, or a modified version of a clone of it
        """
        df_copy = None
        found_cols = False

        for col in df.columns:
            if pd.core.dtypes.common.is_datetime_or_timedelta_dtype(df[col]):
                if not found_cols:
                    df_copy = df.copy()     # First time we see this type of columns

                found_cols = True

                df_copy[col] = df_copy[col].map(
                    lambda x: None if pd.isna(x) else neo4j.time.DateTime.from_native(x)
                )

        if found_cols:
            return df_copy
        else:
            return df





    #####################################################################################################

    '''                                      ~   CSV IMPORT   ~                                       '''

    def ________CSV_IMPORT________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################

    def import_csv_nodes_IN_PROGRESS(self, filename :str, labels :Union[str, List[str], Tuple[str]], os="linux",
                               headers=True, rename_fields=None, field_types=None,
                               import_line_number=False, start_line_number=1, line_number_name="line number",
                               link_to_node=None) -> dict:
        """
        TODO: In-progress.  DO NOT USE.   Use load_pandas() instead
        
        Import data from a CSV file located on the same file system as the Neo4j database,
        and the records represent nodes to be created in the database.

        IMPORTANT:  the file to import MUST be located in the special folder NEO4J_HOME/import,
                    unless the Neo4j configuration file is first modified - and the database restarted!
                    A typical default location, when Neo4j is installed on Linux, is:  /var/lib/neo4j/import

        :param filename:    EXAMPLE in Linux:   "test.csv"
                            EXAMPLE in Windows: "C:/test.csv"
        :param labels:      A string, or list/tuple of strings, representing one or multiple Neo4j labels;
                                it's acceptable to be None
        :param os:          Either "linux" or "win"
        :param headers:         TODO: implement
        :param rename_fields:   TODO: implement
                                    Example: {"name": "Product Name", "n_parts": "Number of Parts"}
        :param field_types: (OPTIONAL) if not specified, all values get imported as STRINGS
                                TODO: implement
                                    Example: {"product_id": "int", "cost": "float"}
        :param import_line_number:
        :param start_line_number:
        :param line_number_name:
        :param link_to_node:    (OPTIONAL) If provided, all the newly-created nodes
                                    get linked to the specified existing node.
                                    Example: {"internal_id": 123, "name": "employed_by", "dir": "out"}

        :return:        A dictionary of statistics about the import
        """
        if os == "linux":
            full_filename = f"/{filename}"
        else:
            full_filename = f"///{filename}"

        labels = CypherUtils.prepare_labels(labels)

        q = f'''
            LOAD CSV WITH HEADERS FROM "file:{full_filename}" AS row
            UNWIND row as properties
            CREATE (n {labels}) SET n = properties
            '''

        if import_line_number:
            offset = start_line_number - 2      # Subtracting 2 because the first line of the file contains the field headers
                                                # (i.e. the count would start with 2, in absence of offset)
            q += f", n.{line_number_name} = linenumber() + {offset}"


        if link_to_node:
            # All the newly-created nodes will get linked to an existing node specified by its internal database ID
            if link_to_node["dir"] == "out":
                link = f"MERGE (n)-[:{link_to_node['name']}]->(l)"
            else:
                link = f"MERGE (n)<-[:{link_to_node['name']}]-(l)"

            q = f'''MATCH (l:CLASS)
                    WHERE id(l) = {link_to_node["internal_id"]}
                    WITH l
                    {q}
                    {link}
                '''


        status = self.update_query(q)
        print(status)
        return status





    #####################################################################################################

    '''                              ~   JSON IMPORT/EXPORT   ~                                       '''

    def ________JSON_IMPORT_EXPORT________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################


    def export_dbase_json(self) -> {}:
        """
        Export the entire Neo4j database as a JSON string.
        TODO: offer an option to automatically include today's date in the name of exported file

        IMPORTANT: APOC must be activated in the database, to use this function.
                   Otherwise it'll raise an Exception

        EXAMPLE:
        { 'nodes': 2,
          'relationships': 1,
          'properties': 6,
          'data': '[{"type":"node","id":"3","labels":["User"],"properties":{"name":"Adam","age":32,"male":true}},\n
                    {"type":"node","id":"4","labels":["User"],"properties":{"name":"Eve","age":18}},\n
                    {"id":"1","type":"relationship","label":"KNOWS","properties":{"since":2003},"start":{"id":"3","labels":["User"]},"end":{"id":"4","labels":["User"]}}\n
                   ]'
        }

        SIDE NOTE: the Neo4j Browser uses a slightly different format for NODES:
                {
                  "identity": 4,
                  "labels": [
                    "User"
                  ],
                  "properties": {
                    "name": "Eve",
                    "age": 18
                  }
                }
              and a substantially more different format for RELATIONSHIPS:
                {
                  "identity": 1,
                  "start": 3,
                  "end": 4,
                  "type": "KNOWS",
                  "properties": {
                    "since": 2003
                  }
                }

        :return:    A dictionary specifying the number of nodes exported ("nodes"),
                    the number of relationships ("relationships"),
                    and the number of properties ("properties"),
                    as well as a "data" field with the actual export as a JSON string
        """
        cypher = '''
            CALL apoc.export.json.all(null, {useTypes: true, stream: true, jsonFormat: "JSON_LINES"})
            YIELD nodes, relationships, properties, data
            RETURN nodes, relationships, properties, data
            '''
        # TODO: unclear if the part "useTypes: true" is needed

        result = self.query(cypher)     # It returns a list with a single element
        export_dict = result[0]         # This will be a dictionary with 4 keys: "nodes", "relationships", "properties", "data"
        #   "nodes", "relationships" and "properties" contain their respective counts

        json_lines = export_dict["data"]
        # Tweak the "JSON_LINES" format to make it a valid JSON string, and more readable.
        # See https://neo4j.com/labs/apoc/4.3/export/json/#export-nodes-relationships-json
        json_str = "[" + json_lines.replace("\n", ",\n ") + "\n]"   # The newlines \n make the JSON much more human-readable
        export_dict["data"] = json_str                              # Replace the "data" value

        #print("export_dict = ", export_dict)

        return export_dict



    def export_nodes_rels_json(self, nodes_query="", rels_query="") -> {}:
        """
        Export the specified nodes, plus the specified relationships, as a JSON string.
        The default empty strings are taken to mean (respectively) ALL nodes/relationships.

        For details on the formats, see export_dbase_json()

        IMPORTANT:  APOC must be activated in the database for this function.
                    Otherwise it'll raise an Exception

        :param nodes_query: A Cypher query to identify the desired nodes (exclusive of RETURN statements)
                                    The dummy variable for the nodes must be "n"
                                    Use "" to request all nodes
                                    EXAMPLE: "MATCH (n) WHERE (n:CLASS OR n:PROPERTY)"
        :param rels_query:   A Cypher query to identify the desired relationships (exclusive of RETURN statements)
                                    The dummy variable for the relationships must be "r"
                                    Use "" to request all relationships (whether or not their end nodes are also exported)
                                    EXAMPLE: "MATCH ()-[r:HAS_PROPERTY]->()"

        :return:    A dictionary specifying the number of nodes exported,
                    the number of relationships, and the number of properties,
                    as well as a "data" field with the actual export as a JSON string

        """
        if nodes_query == "":
            nodes_query = "MATCH (n)"           # All nodes by default

        if rels_query == "":
            rels_query = "MATCH ()-[r]->()"     # All relationships by default

        cypher = f'''
            {nodes_query}
            WITH collect(n) as nds
            OPTIONAL {rels_query}
            WITH nds, collect(r) AS rels
            CALL apoc.export.json.data(nds, rels, null, {{stream: true, jsonFormat: "JSON_LINES"}})
            YIELD nodes, relationships, properties, data
            RETURN nodes, relationships, properties, data
            '''
        # The "OPTIONAL" keyword is necessary in cases where there are no relationships

        # Example of complete Cypher query
        '''
            MATCH (s) WHERE (s:CLASS OR s:PROPERTY) 
            WITH collect(s) as nds
            OPTIONAL MATCH ()-[r:HAS_PROPERTY]->()
            WITH nds, collect(r) AS rels 
            CALL apoc.export.json.data(nds, rels, null, {stream: true, jsonFormat: "ARRAY_JSON"})
            YIELD nodes, relationships, properties, data
            RETURN nodes, relationships, properties, data
        '''

        result = self.query(cypher)     # It returns a list with a single element
        #print("In export_nodes_rels_json(): result = ", result)

        export_dict = result[0]     # This will be a dictionary with 4 keys: "nodes", "relationships", "properties", "data"
        #   "nodes", "relationships" and "properties" contain their respective counts

        json_lines = export_dict["data"]
        # Tweak the "JSON_LINES" format to make it a valid JSON string, and more readable.
        # See https://neo4j.com/labs/apoc/4.3/export/json/#export-nodes-relationships-json
        json_str = "[" + json_lines.replace("\n", ",\n ") + "\n]"       # The newlines \n make the JSON much more human-readable
        export_dict["data"] = json_str                                  # Replace the "data" value

        #print("export_dict = ", export_dict)

        return export_dict




    def is_literal(self, value) -> bool:
        """
        Return True if the given value represents a literal (in terms of database storage)

        :param value:
        :return:
        """
        if type(value) == int or type(value) == float or type(value) == str or type(value) == bool:
            return True
        else:
            return False



    def import_json(self, json_str: str, root_labels="import_root_label", parse_only=False, provenance=None) -> List[int]:
        """
        Import the data specified by a JSON string into the database.

        CAUTION: A "postorder" approach is followed: create subtrees first (with recursive calls), then create root last;
        as a consequence, in case of failure mid-import, there's no top root, and there could be several fragments.
        A partial import might need to be manually deleted.
        TODO: maintain a list of all created nodes - so as to be able to delete them all in case of failure.

        :param json_str:    A JSON string representing the data to import
        :param root_labels: String, or list of strings, to be used as Neo4j labels for the root node(s)
        :param parse_only:  If True, the parsed data will NOT be added to the database
        :param provenance:  Optional string to store in a "source" attribute in the root node
                                (only used if the top-level JSON structure is an object, i.e. if there's a single root node)

        :return:            List of integer ID's (possibly empty), of the root node(s) created
        """
        # Try to obtain Python data (which ought to be a dict or list) that corresponds to the passed JSON string
        try:
            json_data = json.loads(json_str)    # Turn the string (representing JSON data) into its Python counterpart;
            # at the top level, it should be a dict or list
        except Exception as ex:
            raise Exception(f"Incorrectly-formatted JSON string. {ex}")

        #print("Python version of the JSON file:\n"
        #self.debug_trim_print(json_data, max_len = 250)

        if parse_only:
            return []      # Nothing else to do

        # Import the structure into Neo4j
        result = self.create_nodes_from_python_data(json_data, root_labels)

        # TODO: implement a mechanism whereby, if the above call results in error, the partial database structure created gets erased

        if provenance and type(json_data) == dict:       # If provenance is specified, and the top-level JSON structure is a dictionary
            self.debug_print("Stamping the root node of the import with provenance information in the `source` attribute")
            node_id = result[0]
            self.set_fields(node_id, set_dict={"source": provenance})

        return result




    def create_nodes_from_python_data(self, python_data, root_labels: Union[str, List[str]], level=1) -> List[int]:
        """
        Recursive function to add data from a JSON structure to the database, to create a tree:
        either a single node, or a root node with children.
        A "postorder" approach is followed: create subtrees first (with recursive calls), then create root last.

        If the data is a literal, first turn it into a dictionary using a key named "value".

        Return the Neo4j ID's of the root node(s)

        :param python_data: Python data to import.
                                The data can be a literal, or list, or dictionary
                                - and lists/dictionaries may be nested
        :param root_labels: String, or list of strings, to be used as Neo4j labels for the root node(s)
        :param level:       Recursion level (also used for debugging, to make the indentation more readable)
        :return:            List of integer Neo4j internal ID's (possibly empty), of the root node(s) created
        """
        indent_str = self.indent_chooser(level)
        self.debug_print(f"{indent_str}{level}. ~~~~~:")

        if python_data is None:
            self.debug_print(f"{indent_str}Handling a None.  Returning an empty list")
            return []

        # If the data is a literal, first turn it into a dictionary using a key named "value"
        if self.is_literal(python_data):
            # The data is a literal
            original_data = python_data             # Only used for debug-printing, below
            python_data = {"value": python_data}    # Turn the literal data into a dictionary
            self.debug_print(f"{indent_str}Turning literal ({self.debug_trim(original_data, max_len=200)}) into dict, "
                             f"using `value` as key, as follows: {self.debug_trim(python_data)}")

        if type(python_data) == dict:
            self.debug_print(f"{indent_str}Input is a dict with {len(python_data)} key(s): {list(python_data.keys())}")
            new_root_id = self.dict_importer(d=python_data, labels=root_labels, level=level)
            self.debug_print(f"{indent_str}dict_importer returned new_root_id: {new_root_id}")
            return [new_root_id]

        elif type(python_data) == list:
            self.debug_print(f"{indent_str}Input is a list with {len(python_data)} items")
            children_info = self.list_importer(l=python_data, labels=root_labels, level=level)
            if self.debug:
                print(f"{indent_str}children_info: {children_info}")

            if level == 1:
                return children_info    # Top-level lists require a special treatment (no grouping)
            else:
                # Lists that aren't top-level result in element nodes (or subtree roots)
                # that are all attached to a special parent node that has no attributes
                children_list = []
                for child_id in children_info:
                    #children_list.append( (child_id, root_labels) )
                    children_list.append( {"internal_id": child_id, "rel_name":root_labels} )
                self.debug_print(f"{indent_str}Attaching the root nodes of the list elements to a common parent")
                return [self.create_node_with_links(labels=root_labels, properties=None, links=children_list)]

        else:
            raise Exception(f"Unexpected data type: {type(python_data)}")



    def dict_importer(self, d: dict, labels, level: int) -> int:
        """
        Import data from a Python dictionary.
        If the data is nested, it uses a recursive call to create_nodes_from_python_data()
        TODO: pytest

        :param d:       A Python dictionary with data to import
        :param labels:  String, or list of strings, to be used as Neo4j labels for the node
        :param level:   Integer with recursion level (used just to format debugging output)
        :return:        Integer with the internal database id of the newly-created (top-level) node
        """
        indent_str = self.indent_chooser(level)

        node_properties = {}    # Dictionary to be filled in with all the properties of the new node
        children_info = []      # A list of pairs (Neo4j ID, relationship name)

        # Loop over all the dictionary entries
        for k, v in d.items():
            self.debug_trim_print(f"{indent_str}*** KEY-> VALUE: {k} -> {v}")

            if self.is_literal(v):
                node_properties[k] = v      # Add the key/value to the running list of properties of the new node
                if self.debug:
                    print(f"{indent_str}The value (`{v}`) is a literal of type {type(v)}. Node properties so far: {node_properties}")
                else:
                    print(f"{indent_str}The value (`{self.debug_trim(v)}`) is a literal of type {type(v)}")      # Concise version

            elif type(v) == dict:
                self.debug_print(f"{indent_str}Processing a dictionary (with {len(v)} keys), using a recursive call:")
                # Recursive call
                new_node_id_list = self.create_nodes_from_python_data(python_data=v, root_labels=k, level=level + 1)
                if len(new_node_id_list) > 1:
                    raise Exception("Internal error: processing a dictionary is returning more than 1 root node")
                elif len(new_node_id_list) == 1:
                    new_node_id = new_node_id_list[0]
                    children_info.append( {"internal_id": new_node_id, "rel_name": k} )  # Append dict entry to the running list
                # Note: if the list is empty, do nothing

            elif type(v) == list:
                self.debug_print(f"{indent_str}Processing a list (with {len(v)} elements):")
                new_children = self.list_importer(l=v, labels=k, level=level)
                for child_id in new_children:
                    children_info.append( {"internal_id": child_id, "rel_name": k} )     # Append dict entry to the running list

            # Note: if v is None, no action is taken.  Dictionary entries with values of None are disregarded


        self.debug_print(f"{indent_str}dict_importer assembled node_properties: {node_properties} | children_info: {children_info}")
        return self.create_node_with_links(labels=labels, properties=node_properties, links=children_info)



    def list_importer(self, l: list, labels, level) -> [int]:
        """
        Import data from a list.
        If the data is nested, it uses a recursive call to create_nodes_from_python_data()
        TODO: pytest

        :param l:       A list with data to import
        :param labels:  String, or list of strings, to be used as Neo4j labels for the node
        :param level:   Integer with recursion level (just used to format debugging output)
        :return:        List (possibly empty) of internal database id's of the newly-created nodes
        """
        indent_str = self.indent_chooser(level)
        if len(l) == 0:
            self.debug_print(f"{indent_str}The list is empty; so, ignoring it (Returning an empty list)")
            return []

        list_of_child_ids = []
        # Process each element of the list, in turn
        for item in l:
            self.debug_print(f"{indent_str}Making recursive call to process list element...")
            new_node_id_list = self.create_nodes_from_python_data(python_data=item, root_labels=labels, level=level + 1)  # Recursive call
            list_of_child_ids += new_node_id_list   # List concatenation

        if self.debug:
            print(f"{indent_str}list_importer() is returning: {list_of_child_ids}")

        return list_of_child_ids



    def import_json_dump(self, json_str: str, extended_validation = True) -> str:
        """
        Used to import data from a database dump that was done with export_dbase_json() or export_nodes_rels_json().

        Import nodes and relationships into the database, as specified in the JSON code
        that was created by the earlier data dump.

        IMPORTANT: the internal id's of the nodes need to be shifted,
              because one cannot force the Neo4j internal id's to be any particular value...
              and, besides (if one is importing into an existing database), particular id's may already be taken.

        :param json_str:            A JSON string with the format specified under export_dbase_json()
        :param extended_validation: If True, an attempt is made to try to avoid partial imports,
                                        by running extended validations prior to importing
                                        (it will make a first pass thru the data, and hence take longer)

        :return:                    A status message with import details if successful;
                                        or raise an Exception if not.
                                        If an error does occur during import then the import is aborted -
                                        and the number of imported nodes & relationships is returned in the Exception raised.
        """

        try:
            json_list = json.loads(json_str)    # Turn the string (which represents a JSON list) into a list
        except Exception as ex:
            raise Exception(f"import_json_dump(): incorrectly-formatted JSON string. {ex}")

        if self.debug:
            print("json_list: ", json_list)

        assert type(json_list) == list, \
            "import_json_dump(): the JSON string does not represent a list"


        id_shifting = {}    # To map the Neo4j internal ID's specified in the JSON data dump
        #       into the ID's of newly-created nodes

        if extended_validation:
            # Do an initial pass for correctness, to help avoid partial imports.
            # TODO: maybe also check the validity of the start and end nodes of relationships
            for i, item in enumerate(json_list):
                assert type(item) == dict, \
                    f"import_json_dump(): Item in list index {i} should be a dict, but instead it's of type {type(item)}.  Nothing imported.  Item: {item}"
                # We use item.get(key_name) to handle without error situation where the key is missing
                if (item.get("type") != "node") and (item.get("type") != "relationship"):
                    raise Exception(f"import_json_dump(): Item in list index {i} must be a dict with a 'type' key, "
                                    f"whose value is either 'node' or 'relationship'.  Nothing imported.  Item: {item}")

                if item["type"] == "node":
                    if "id" not in item:
                        raise Exception(f"import_json_dump(): Item in list index {i} is marked as 'node' but it lacks an 'id'.  Nothing imported.  Item: {item}")
                    try:
                        int(item["id"])
                    except ValueError:
                        raise Exception(f"import_json_dump(): Item in list index {i} has an 'id' key whose value ({item['id']}) doesn't correspond to an integer.  "
                                        f"Nothing imported.  Item: {item}")

                elif item["type"] == "relationship":
                    if "label" not in item:
                        raise Exception(f"import_json_dump(): Item in list index {i} is marked as 'relationship' but lacks a 'label'.  Nothing imported.  Item: {item}")
                    if "start" not in item:
                        raise Exception(f"import_json_dump(): Item in list index {i} is marked as 'relationship' but lacks a 'start' value.  Nothing imported.  Item: {item}")
                    if "end" not in item:
                        raise Exception(f"import_json_dump(): Item in list index {i} is marked as 'relationship' but lacks a 'end' value.  Nothing imported.  Item: {item}")
                    if "id" not in item["start"]:
                        raise Exception(f"import_json_dump(): Item in list index {i} is marked as 'relationship' but its 'start' value lacks an 'id'.  Nothing imported.  Item: {item}")
                    if "id" not in item["end"]:
                        raise Exception(f"import_json_dump(): Item in list index {i} is marked as 'relationship' but its 'end' value lacks an 'id'.  Nothing imported.  Item: {item}")


        # First, process all the node data, and create the nodes; while doing that, generate the id_shifting map
        num_nodes_imported = 0
        try:
            for item in json_list:
                if item["type"] == "node":
                    #print("ADDING NODE: ", item)
                    #print(f'     Creating node with labels `{item["labels"]}` and properties {item["properties"]}')
                    old_id = int(item["id"])
                    new_id = self.create_node(labels=item.get("labels"), properties=item.get("properties")) # Note: any number of labels can be imported
                                                                                                            #       if item has no labels/properties, None will be passed
                    id_shifting[old_id] = new_id
                    num_nodes_imported += 1
        except Exception as ex:
            raise Exception(f"import_json_dump(): the import process was INTERRUPTED "
                            f"after importing {num_nodes_imported} node(s) and 0 relationship(s). Reason: " + str(ex))


        #print("id_shifting map:", id_shifting)

        # Then process all the relationships, linking to the correct (newly-created) nodes by using the id_shifting map
        # (node: item types that aren't either "node" nor "relationship" are currently being ignored during the import)
        num_rels_imported = 0
        try:
            for item in json_list:
                if item["type"] == "relationship":
                    #print("ADDING RELATIONSHIP: ", item)
                    rel_name = item["label"]
                    #rel_props = item["properties"]
                    rel_props = item.get("properties")      # Also works if no "properties" is present (relationships may lack it)

                    start_id_original = int(item["start"]["id"])
                    end_id_original = int(item["end"]["id"])

                    if start_id_original not in id_shifting:
                        raise Exception(f"cannot add a relationship `{rel_name}` starting at node with id {start_id_original}, because no node with that id was imported")
                    if end_id_original not in id_shifting:
                        raise Exception(f"cannot add a relationship `{rel_name}` ending at node with id {start_id_original}, because no node with that id was imported")

                    start_id_shifted = id_shifting[start_id_original]
                    end_id_shifted = id_shifting[end_id_original]

                    #print(f'     Creating relationship named `{rel_name}` from node {start_id_shifted} to node {end_id_shifted},  with properties {rel_props}')
                    self.link_nodes_by_ids(start_id_shifted, end_id_shifted, rel_name, rel_props)
                    num_rels_imported += 1
        except Exception as ex:
            raise Exception(f"import_json_dump(): the import process was INTERRUPTED "
                            f"after importing {num_nodes_imported} node(s) and {num_rels_imported} relationship(s). Reason: " + str(ex))


        return f"Successful import of {num_nodes_imported} node(s) and {num_rels_imported} relationship(s)"




    #####################################################################################################

    '''                                   ~   DEBUGGING SUPPORT   ~                                   '''

    def ________DEBUGGING_SUPPORT________(DIVIDER):
        pass        # Used to get a better structure view in IDEs
    #####################################################################################################


    def debug_print(self, info: str, trim=False) -> None:
        """
        If the class' property "debug" is set to True,
        print out the passed info string,
        optionally trimming it, if too long

        :param info:
        :param trim:
        :return:        None
        """
        if self.debug:
            if trim:
                info = self.debug_trim(info)

            print(info)



    def debug_trim(self, data, max_len = 150) -> str:
        """
        Abridge the given data (first turning it into a string if needed), if excessively long,
        using ellipses " ..." for the omitted data.
        Return the abridged data.

        :param data:    String with data to possibly abridge
        :param max_len: Max number of characters to show from the data argument
        :return:        The (possibly) abridged text
        """
        text = str(data)
        if len(text) > max_len:
            return text[:max_len] + " ..."
        else:
            return text


    def debug_trim_print(self, data, max_len = 150) -> None:
        """
        Abridge the given data (first turning it into a string if needed),
        if it is excessively long; then print it

        :param data:    String with data to possibly abridge, and then print
        :param max_len: Max number of characters to show from the data argument
        :return:        None
        """
        print(self.debug_trim(data, max_len))



    def indent_chooser(self, level: int) -> str:
        """
        Create an indent based on a "level": handy for debugging recursive functions

        :param level:
        :return:
        """
        indent_spaces = level*4
        indent_str = " " * indent_spaces        # Repeat a blank character the specified number of times
        return indent_str



    def _debug_local(self) -> str:
        """
        Use to test the switch from a local to remote repository, for debugging

        :return:
        """
        return "local"



