####  WARNING : the Neo4j database identified by the environment variables below, will get erased!!!

"""
IMPORTANT - to run the pytests in this file, the following ENVIRONMENT VARIABLES must first be set:
                1. NEO4J_HOST
                2. NEO4J_USER
                3. NEO4J_PASSWORD

            For example, if using PyCharm, follow the main menu to: Run > Edit Configurations
            and then, in the template for pytest, set Environment Variable to something like:
                    NEO4J_HOST=bolt://<your IP address>:7687;NEO4J_USER=neo4j;NEO4J_PASSWORD=<your Neo4j password>
"""

from cProfile import label
import pytest
from src.neptuneaccess import neptuneaccess as neptuneaccess
from utilities.comparisons import compare_unordered_lists, compare_recordsets
from datetime import datetime, date
import os
import pandas as pd
import numpy as np
import neo4j.time

# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def db():
    # MAKE SURE TO FIRST SET THE ENVIRONMENT VARIABLES, prior to run the pytests in this file!
    neo_obj = neptuneaccess.NeptuneAccess(host=os.environ.get("NEPTUNE_HOST"),credentials=("username","password"),debug=False)     # Change the debug option to True if desired
    yield neo_obj


#############  METHODS from the Parent Class NeoAccessCore  #################

##  ~ INIT ~
def test_construction():
    # Note: if database isn't running, the error output includes the line:
    """
        Exception: CHECK IF NEO4J IS RUNNING! While instantiating the NeoAccess object,
        failed to create the driver: Unable to retrieve routing information
    """

    # MAKE SURE TO FIRST SET THE ENVIRONMENT VARIABLES
    url = os.environ.get("NEPTUNE_HOST")

    credentials_name = os.environ.get("NEPTUNE_USERNAME")
    credentials_pass = os.environ.get("NEPTUNE_PASSWORD")

    credentials_as_tuple = (credentials_name, credentials_pass)
    credentials_as_list = [credentials_name, credentials_pass]


    # One way of instantiating the class
    obj1 = neptuneaccess.NeptuneAccess(url, debug=False)       # Rely on default username/pass

    assert obj1.debug is False
    assert obj1.version() == "4.4.11"    # Test the version of the Neo4j driver (this ought to match the value in requirements.txt)


    # Another way of instantiating the class
    obj2 = neptuneaccess.NeptuneAccess(url, credentials_as_tuple, debug=False) # Explicitly pass the credentials
    assert obj2.driver is not None


    # Yet another way of instantiating the class
    obj3 = neptuneaccess.NeptuneAccess(url, credentials_as_list, debug=False) # Explicitly pass the credentials
    assert obj3.driver is not None


    # removing this testcase , since neptune connectivity is independent of the credentials and only uri will work
    
    # with pytest.raises(Exception):
    #     assert neo_access.NeoAccess(url, "bad_credentials", debug=False)    # This ought to raise an Exception


def test_connect(db):
    db.connect()
    assert db.driver is not None

def test_version(db):
    assert type (db.version()) == str
    assert db.version() != ""

###  ~ RUNNING GENERIC QUERIES ~

def test_query(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)  #patch fix to avoid apoc issue
    q = "CREATE (:car {make:'Toyota', color:'white'})"   # Create a node without returning it
    result = db.query(q)
    assert result == []

    q = "CREATE (n:car {make:'VW', color:$color, year:2021}) RETURN n"   # Create a node and return it; use data binding
    result = db.query(q, {"color": "red"})
    assert result == [{'n': {'color': 'red', 'make': 'VW', 'year': 2021}}]

    q = "MATCH (x) RETURN x"
    result = db.query(q)
    expected = [{'x': {'color': 'white', 'make': 'Toyota'}},
                {'x': {'color': 'red', 'year': 2021, 'make': 'VW'}}]
    assert compare_recordsets(result, expected)

    q = '''CREATE (b:boat {number_masts: 2, year:2003}),
                  (c:car {color: "blue"})
           RETURN b, c
        '''   # Create and return multiple nodes
    result = db.query(q)
    assert result == [{'b': {'number_masts': 2, 'year': 2003},
                       'c': {'color': 'blue'}}]

    q = "MATCH (c:car {make:'Toyota'}) RETURN c"
    result = db.query(q)
    assert result == [{'c': {'color': 'white', 'make': 'Toyota'}}]

    q = "MATCH (c:car) RETURN c.color, c.year AS year_made"
    result = db.query(q)
    expected = [{'c.color': 'white', 'year_made': None},
                {'c.color': 'red', 'year_made': 2021},
                {'c.color': 'blue', 'year_made': None}]
    assert compare_recordsets(result, expected)

    q = "MATCH (c:car) RETURN count(c)"
    result = db.query(q)
    assert result == [{"count(c)": 3}]

    q = '''MATCH (c:car {make:'Toyota', color:'white'}) 
           MERGE (c)-[r:bought_by {price:7500}]->(p:person {name:'Julian'})
           RETURN r
        '''
    result = db.query(q)
    assert result == [{'r': ({}, 'bought_by', {})}]



def test_query_2(db):
    # Explore the "single_row" and "single_cell" arguments of query()

    db.empty_dbase(drop_indexes=False, drop_constraints=False)  #to avoid the apoc issue --> original true in both cases

    q = "MATCH (n) RETURN n"    # Query to locate all nodes

    result = db.query(q, single_row=True)
    assert result is None       # No records returned

    result = db.query(q, single_cell="some_name")
    assert result is None       # No records returned


    db.create_node(labels="car", properties={})     # A record with no properties

    result = db.query(q, single_row=True)
    assert result == {'n': {}}      # Note how this differs from the case of no records found

    result = db.query(q, single_cell="some_name")
    assert result is None           # A record was found, but lacks a field called "some_name"


    q = "MATCH (n) RETURN n.color AS color"
    result = db.query(q, single_row=True)
    assert result == {"color": None}
    result = db.query(q, single_cell="color")
    assert result is None


    # Add a 1st boat
    db.create_node(labels="boat", properties={"type": "sloop"})
    q = "MATCH (n :boat) RETURN n.type AS boat_type"
    result = db.query(q, single_row=True)
    assert result == {"boat_type": "sloop"}
    result = db.query(q, single_cell="boat_type")
    assert result == "sloop"
    result = db.query(q, single_cell="brand")
    assert result is None   # No "brand" field in the result

    # Add a 2nd boat
    db.create_node(labels="boat", properties={"type": "ketch"})
    q = "MATCH (n :boat) RETURN n.type AS boat_type ORDER BY n.type"
    result = db.query(q, single_row=True)
    assert result == {"boat_type": "ketch"}
    result = db.query(q, single_cell="boat_type")
    assert result == "ketch"
    result = db.query(q, single_cell="brand")
    assert result is None   # Still no "brand" field in the result



def test_query_extended(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # Create and return 1st node
    q = "CREATE (n:car {make:'Toyota', color:'white'}) RETURN n"
    result = db.query_extended(q, flatten=True)
    white_car_id = result[0]['internal_id']
    assert type(white_car_id) == int
    assert result == [{'color': 'white', 'make': 'Toyota', 'neo4j_labels': ['car'], 'internal_id': white_car_id}]

    q = "MATCH (x) RETURN x"
    result = db.query_extended(q, flatten=True)
    assert result == [{'color': 'white', 'make': 'Toyota', 'neo4j_labels': ['car'], 'internal_id': white_car_id}]

    # Create and return 2 more nodes at once
    q = '''CREATE (b:boat {number_masts: 2, year:2003}),
                  (c:car {color: "blue"})
           RETURN b, c
        '''
    result = db.query_extended(q, flatten=True)
    for node_dict in result:
        if node_dict['neo4j_labels'] == ['boat']:
            boat_id = node_dict['internal_id']
        else:
            blue_car_id = node_dict['internal_id']

    assert result == [{'number_masts': 2, 'year': 2003, 'neo4j_labels': ['boat'], 'internal_id': boat_id},
                      {'color': 'blue', 'neo4j_labels': ['car'], 'internal_id': blue_car_id}]

    # Retrieve all 3 nodes at once
    q = "MATCH (x) RETURN x"
    result = db.query_extended(q, flatten=True)
    expected = [{'color': 'white', 'make': 'Toyota', 'neo4j_labels': ['car'], 'internal_id': white_car_id},
                {'number_masts': 2, 'year': 2003, 'neo4j_labels': ['boat'], 'internal_id': boat_id},
                {'color': 'blue', 'neo4j_labels': ['car'], 'internal_id': blue_car_id}]
    assert compare_recordsets(result, expected)

    q = "MATCH (b:boat), (c:car) RETURN b, c"
    result = db.query_extended(q, flatten=True)
    expected = [{'number_masts': 2, 'year': 2003, 'internal_id': boat_id, 'neo4j_labels': ['boat']},
                {'color': 'white', 'make': 'Toyota', 'internal_id': white_car_id, 'neo4j_labels': ['car']},
                {'number_masts': 2, 'year': 2003, 'internal_id': boat_id, 'neo4j_labels': ['boat']},
                {'color': 'blue', 'internal_id': blue_car_id, 'neo4j_labels': ['car']}]
    assert compare_recordsets(result, expected)

    result = db.query_extended(q, flatten=False)    # Same as above, but without flattening
    assert len(result) == 2
    expected_0 = [{'number_masts': 2, 'year': 2003, 'internal_id': boat_id, 'neo4j_labels': ['boat']},
                  {'color': 'white', 'make': 'Toyota', 'internal_id': white_car_id, 'neo4j_labels': ['car']}
                 ]
    expected_1 = [{'number_masts': 2, 'year': 2003, 'internal_id': boat_id, 'neo4j_labels': ['boat']},
                  {'color': 'blue', 'internal_id': blue_car_id, 'neo4j_labels': ['car']}
                 ]

    if compare_recordsets(result[0], expected_0):           # If the list elements at the top level are in the same order
        assert compare_recordsets(result[1], expected_1)
    else:                                                   # If the list elements at the top level are in reverse order
        assert compare_recordsets(result[0], expected_1)
        assert compare_recordsets(result[1], expected_0)


    # Create and retrieve a new relationship, with attributes
    q = '''MATCH (c:car {make:'Toyota', color:'white'}) 
           MERGE (c)-[r:bought_by {price:7500}]->(p:person {name:'Julian'})
           RETURN r
        '''
    result = db.query_extended(q, flatten=True)
    # EXAMPLE of result:
    #   [{'price': 7500, 'internal_id': 1, 'neo4j_start_node': <Node id=11 labels=frozenset() properties={}>, 'neo4j_end_node': <Node id=14 labels=frozenset() properties={}>, 'neo4j_type': 'bought_by'}]

    # Side tour to get the Neo4j id of the "person" name created in the process
    look_up_person = "MATCH (p:person {name:'Julian'}) RETURN p"
    person_result = db.query_extended(look_up_person, flatten=True)
    person_id = person_result[0]['internal_id']

    assert len(result) == 1
    rel_data = result[0]
    assert rel_data['neo4j_type'] == 'bought_by'
    assert rel_data['price'] == 7500
    assert type(rel_data['internal_id']) == int
    assert rel_data['neo4j_start_node'].id == white_car_id
    assert rel_data['neo4j_end_node'].id == person_id

    # A query that returns both nodes and relationships
    q = '''MATCH (c:car {make:'Toyota'}) 
                 -[r:bought_by]->(p:person {name:'Julian'})
           RETURN c, r, p
        '''
    result = db.query_extended(q, flatten=True)
    assert len(result) == 3     # It returns a car, a person, and a relationship

    for item in result:
        if item.get('color') == 'white':    # It's the car node
            assert item == {'color': 'white', 'make': 'Toyota', 'internal_id': white_car_id, 'neo4j_labels': ['car']}
        elif item.get('name') == 'Julian':     # It's the person node
            assert item == {'name': 'Julian', 'internal_id': person_id, 'neo4j_labels': ['person']}
        else:                                   # It's the relationship
            assert item['neo4j_type'] == 'bought_by'
            assert item['price'] == 7500
            assert type(item['internal_id']) == int
            assert item['neo4j_start_node'].id == white_car_id
            assert item['neo4j_end_node'].id == person_id


    # Queries that return values rather than Graph Data Types such as nodes and relationships
    q = "MATCH (c:car) RETURN c.color, c.year AS year_made"
    result = db.query(q)
    expected = [{'c.color': 'white', 'year_made': None},
                {'c.color': 'blue', 'year_made': None}]
    assert compare_recordsets(result, expected)

    q = "MATCH (c:car) RETURN count(c)"
    result = db.query(q)
    assert result == [{"count(c)": 2}]




#################  METHODS from the Child Class NeoAccess  #################

def test_update_query(db):
    pass    # TODO




###  ~ RETRIEVE DATA ~

def test_get_single_field(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)
    # Create 2 nodes
    db.query('''CREATE (:`my label`:`color` {`field A`: 123, `field B`: 'test'}), 
                       (:`my label`:`make`  {                `field B`: 'more test', `field C`: 3.14})
             ''')

    match = db.match(labels="my label")

    result = db.get_single_field(match=match, field_name="field A")
    assert compare_unordered_lists(result, [123, None])

    result = db.get_single_field(match=match, field_name="field B")
    assert compare_unordered_lists(result, ['test', 'more test'])

    match = db.match(labels="make")

    result = db.get_single_field(match=match, field_name="field C")
    assert compare_unordered_lists(result, [3.14])

    match = db.match(labels="")      # No labels specified
    result = db.get_single_field(match=match, field_name="field C")
    assert compare_unordered_lists(result, [None, 3.14])



def test_get_record_by_primary_key(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    node_id_Valerie = db.create_node("person", {'SSN': 123, 'name': 'Valerie', 'gender': 'F'})
    db.create_node("person", {'SSN': 456, 'name': 'Therese', 'gender': 'F'})


    assert db.get_record_by_primary_key("person", primary_key_name="SSN", primary_key_value=123) \
           == {'SSN': 123, 'name': 'Valerie', 'gender': 'F'}
    assert {
    'SSN': 123,
    'gender': 'F',
    'internal_id': 503331086,
    'name': 'Valerie',
    'neo4j_labels': ['person']} == {
    'SSN': 123,
    'gender': 'F',
    'internal_id': 503331086,
    'name': 'Valerie',
    'neo4j_labels': ['person']}


    assert db.get_record_by_primary_key("person", primary_key_name="SSN", primary_key_value=456) \
           == {'SSN': 456, 'name': 'Therese', 'gender': 'F'}


    db.create_node("person", {'SSN': 456, 'name': 'Therese clone', 'gender': 'F'})  # Irregular situation with 2 records sharing what
                                                                                    # was meant to be a primary key
    with pytest.raises(Exception):
        db.get_record_by_primary_key("person", primary_key_name="SSN", primary_key_value=456)


    # Now, try to find records that don't exist
    assert db.get_record_by_primary_key("person", primary_key_name="SSN", primary_key_value=99999) is None   # Not found
    assert db.get_record_by_primary_key("wrong_label", primary_key_name="SSN", primary_key_value=123) is None   # Not found
    assert db.get_record_by_primary_key("person", primary_key_name="bad_key", primary_key_value=123) is None   # Not found



def test_exists_by_key(db):
    db.empty_dbase()

    assert not db.exists_by_key("person", key_name="SSN", key_value=123)    # Cannot exist, because we just emptied the database

    db.create_node("person", {'SSN': 123, 'name': 'Valerie', 'gender': 'F'})
    db.create_node("person", {'SSN': 456, 'name': 'Therese', 'gender': 'F'})

    assert db.exists_by_key("person", key_name="SSN", key_value=123)
    assert db.exists_by_key("person", key_name="SSN", key_value=456)
    assert db.exists_by_key("person", key_name="name", key_value='Valerie')

    assert not db.exists_by_key("person", key_name="SSN", key_value=5555)
    assert not db.exists_by_key("person", key_name="name", key_value='Joe')
    assert not db.exists_by_key("non_existent_label", key_name="SSN", key_value=123)



def test_exists_by_internal_id(db):
    #todo: not working for int ids... it will work if we use hex internal ids
    db.empty_dbase(drop_constraints=False,drop_indexes=False)

    assert not db.exists_by_internal_id(internal_id = 8888)     # Cannot exist, because we just emptied the database

    Valerie_ID = db.create_node("person", {'SSN': 123, 'name': 'Valerie', 'gender': 'F'})
    valerie_hex_id=db.query("CREATE (n:`person` {`SSN`: 123, `name`: 'Valerie', `gender`: 'F'}) RETURN id(n) AS hex_id")[0].get("hex_id")

    assert db.exists_by_internal_id(valerie_hex_id)


    Therese_ID = db.create_node("person", {'SSN': 456, 'name': 'Therese', 'gender': 'F'})
    therese_hex_id=db.query("CREATE (n:`person` {`SSN`: 456, `name`: 'Therese', `gender`: 'F'}) RETURN id(n) AS hex_id")[0].get("hex_id")
    assert db.exists_by_internal_id(therese_hex_id)




def test_get_nodes(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # Create a 1st new node
    db.create_node("test_label", {'patient_id': 123, 'gender': 'M'})

    # Retrieve the record just created (using values embedded in the Cypher query)
    match = db.match(labels="test_label", clause="n.patient_id = 123 AND n.gender = 'M'")
    retrieved_records = db.get_nodes(match)
    assert retrieved_records == [{'patient_id': 123, 'gender': 'M'}]

    # Again, retrieve the record (this time using data-binding in the Cypher query, and values passed as a separate dictionary)
    match = db.match(labels="test_label",
                     clause=( "n.patient_id = $patient_id AND n.gender = $gender",
                               {"patient_id": 123, "gender": "M"}
                            )
                    )
    retrieved_records = db.get_nodes(match)
    assert retrieved_records == [{'patient_id': 123, 'gender': 'M'}]


    # Retrieve ALL records with the label "test_label", by using no clause or an empty clause
    match = db.match(labels="test_label")
    retrieved_records = db.get_nodes(match)
    assert retrieved_records == [{'patient_id': 123, 'gender': 'M'}]

    match = db.match(labels="test_label", clause="           ")
    retrieved_records = db.get_nodes(match)
    assert retrieved_records == [{'patient_id': 123, 'gender': 'M'}]


    # Create a 2nd new node, using a BLANK in an attribute key
    db.create_node("my 2nd label", {'age': 21, 'gender': 'F', 'client id': 123})

    # Retrieve the record just created (using values embedded in the Cypher query)
    match = db.match(labels="my 2nd label", clause="n.`client id` = 123")
    retrieved_records = db.get_nodes(match)
    assert retrieved_records == [{'age': 21, 'gender': 'F', 'client id': 123}]

    # Retrieve the record just created (in a different way, using a Cypher subquery with its own data binding)
    match = db.match(labels="my 2nd label", clause=("n.age = $age AND n.gender = $gender",
                                                     {"age": 21, "gender": "F"}
                                                     ))
    retrieved_records = db.get_nodes(match)
    assert retrieved_records == [{'age': 21, 'gender': 'F', 'client id': 123}]

    # Retrieve ALL records with the label "my 2nd label"
    match = db.match(labels="my 2nd label")
    retrieved_records = db.get_nodes(match)
    assert retrieved_records == [{'age': 21, 'gender': 'F', 'client id': 123}]

    # Same as above, but using a blank subquery
    match = db.match(labels="my 2nd label", clause="           ")
    retrieved_records = db.get_nodes(match)
    assert retrieved_records == [{'age': 21, 'gender': 'F', 'client id': 123}]

    # Retrieve the record just created (using a dictionary of properties)
    match = db.match(labels="my 2nd label", properties={"age": 21, "gender": "F"})
    retrieved_records = db.get_nodes(match)
    assert retrieved_records == [{'client id': 123, 'gender': 'F', 'age': 21}]


    # Add a 2nd new node
    db.create_node("my 2nd label", {'age': 30, 'gender': 'M', 'client id': 999})

    # Retrieve records using a clause
    match = db.match(labels="my 2nd label", clause="n.age > 22")
    retrieved_records = db.get_nodes(match)
    assert retrieved_records == [{'gender': 'M', 'client id': 999, 'age': 30}]

    # Retrieve nodes REGARDLESS of label (and also retrieve the labels)
    match = db.match(properties={"gender": "M"})       # Locate all males, across all node labels
    retrieved_records = db.get_nodes(match, return_labels=True)
    expected_records = [{'neo4j_labels': ['test_label'], 'gender': 'M', 'patient_id': 123},
                        {'neo4j_labels': ['my 2nd label'], 'client id': 999, 'gender': 'M', 'age': 30}]
    assert compare_recordsets(retrieved_records, expected_records)

    # Retrieve ALL nodes in the database (and also retrieve the labels)
    match = db.match()
    retrieved_records = db.get_nodes(match, return_labels=True)
    expected_records = [{'neo4j_labels': ['test_label'], 'gender': 'M', 'patient_id': 123},
                        {'neo4j_labels': ['my 2nd label'], 'client id': 999, 'gender': 'M', 'age': 30},
                        {'neo4j_labels': ['my 2nd label'], 'client id': 123, 'gender': 'F', 'age': 21}]
    assert compare_recordsets(retrieved_records, expected_records)

    # Re-use of same key names in subquery data-binding and in properties dictionaries is ok, because the keys in
    #   properties dictionaries get internally changed
    match = db.match(labels="my 2nd label", clause=("n.age > $age" , {"age": 22}), properties={"age": 30})
    retrieved_records = db.get_nodes(match)
    # Note: internally, the final Cypher query is: "MATCH (n :`my 2nd label` {`age`: $n_par_1}) WHERE (n.age > $age) RETURN n"
    #                           with data binding: {'age': 22, 'n_par_1': 30}
    # The joint requirement (age > 22 and age = 30) lead to the following record:
    expected_records = [{'gender': 'M', 'client id': 999, 'age': 30}]
    assert compare_recordsets(retrieved_records, expected_records)

    # A conflict arises only if we happen to use a key name that clashes with an internal name, such as "n_par_1";
    # an Exception is expected is such a case
    with pytest.raises(Exception):
        match = db.match(labels="my 2nd label", clause=("n.age > $n_par_1" , {"n_par_1": 22}), properties={"age": 30})
        assert db.get_nodes(match)

    # If we really wanted to use a key called "n_par_1" in our subquery dictionary, we
    #       can simply alter the dummy name internally used, from the default "n" to something else
    match = db.match(dummy_node_name="person", labels="my 2nd label",
                     clause=("person.age > $n_par_1" , {"n_par_1": 22}), properties={"age": 30})
    # All good now, because internally the Cypher query is "MATCH (person :`my 2nd label` {`age`: $person_par_1}) WHERE (person.age > $n_par_1) RETURN person"
    #                                    with data binding {'n_par_1': 22, 'person_par_1': 30}
    retrieved_records = db.get_nodes(match)
    assert compare_recordsets(retrieved_records, expected_records)


    # Now, do a clean start, and investigate a list of nodes that differ in attributes (i.e. nodes that have different lists of keys)

    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # Create a first node, with attributes 'age' and 'gender'
    db.create_node("patient", {'age': 16, 'gender': 'F'})

    # Create a second node, with attributes 'weight' and 'gender' (notice the PARTIAL overlap in attributes with the previous node)
    db.create_node("patient", {'weight': 155, 'gender': 'M'})

    # Retrieve combined records created: note how different records have different keys
    match = db.match(labels="patient")
    retrieved_records = db.get_nodes(match)
    expected = [{'gender': 'F', 'age': 16},
                {'gender': 'M', 'weight': 155}]
    assert compare_recordsets(retrieved_records, expected)

    # Add a node with no attributes
    empty_record_id = db.create_node("patient").get("internal_id")
    retrieved_records = db.get_nodes(match)
    expected = [{'gender': 'F', 'age': 16},
                {'gender': 'M', 'weight': 155},
                {}]
    assert compare_recordsets(retrieved_records, expected)

    match = db.match(labels="patient", properties={"age": 16})
    retrieved_single_record = db.get_nodes(match, single_row=True)
    assert retrieved_single_record == {'gender': 'F', 'age': 16}

    match = db.match(labels="patient", properties={"age": 11})
    retrieved_single_record = db.get_nodes(match, single_row=True)
    assert retrieved_single_record is None      # No record found

    match = db.match(labels="patient", internal_id=empty_record_id)
    retrieved_single_record = db.get_nodes(match, single_row=True)
    print('retrieved_single_record - - - - -',retrieved_single_record)
    assert retrieved_single_record == None        # Record with no attributes found



def test_get_df(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # Create and load a test Pandas dataframe with 2 columns and 2 rows
    df_original = pd.DataFrame({"patient_id": [1, 2], "name": ["Jack", "Jill"]})
    db.load_pandas(df_original, labels="A")

    match = db.match(labels="A")
    df_new = db.get_df(match=match)

    # Sort the columns and then sort the rows, in order to disregard both row and column order (TODO: turn into utility)
    df_original_sorted = df_original.sort_index(axis=1)
    df_original_sorted = df_original_sorted.sort_values(by=df_original_sorted.columns.tolist()).reset_index(drop=True)

    df_new_sorted = df_new.sort_index(axis=1)
    df_new_sorted = df_new_sorted.sort_values(by=df_new_sorted.columns.tolist()).reset_index(drop=True)

    assert df_original_sorted.equals(df_new_sorted)



def test_get_node_labels(db):
    db.empty_dbase()

    my_book = db.create_node("book", {'title': 'The Double Helix'})
    my_book_labels= my_book.get("neo4j_labels")
    assert compare_unordered_lists(my_book_labels,['book'])

    my_vehicle = db.create_node(["car", "vehicle"])
    my_vehicle_labels= my_vehicle.get("neo4j_labels")
    assert compare_unordered_lists(my_vehicle_labels,["car", "vehicle"])

    label_less = db.create_node(labels=None, properties={'title': 'I lost my labels'})
    label_less_labels = label_less.get("neo4j_labels")
    assert label_less_labels == ['vertex']




##  ~ FOLLOW LINKS ~

def test_follow_links(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    db.create_node("book", {'title': 'The Double Helix'})
    db.create_node("book", {'title': 'Intro to Hilbert Spaces'})

    # Create new node, linked to the previous two
    db.create_node_with_relationships(labels="person", properties={"name": "Julian", "city": "Berkeley"},
                                      connections=[
                                          {"labels": "book",
                                           "key": "title", "value": "The Double Helix",
                                           "rel_name": "OWNS", "rel_dir": "OUT"},
                                          {"labels": "book",
                                           "key": "title", "value": "Intro to Hilbert Spaces",
                                           "rel_name": "OWNS", "rel_dir": "OUT"}
                                      ]
                                      )

    match = db.match(labels="person", properties={"name": "Julian", "city": "Berkeley"})

    links = db.follow_links(match, rel_name="OWNS", rel_dir="OUT", neighbor_labels="book")
    expected = [{'title': 'The Double Helix'} , {'title': 'Intro to Hilbert Spaces'} ]
    assert compare_recordsets(links, expected)



def test_count_links(db):
    db.empty_dbase()

    db.create_node("book", {'title': 'The Double Helix'})
    db.create_node("book", {'title': 'Intro to Hilbert Spaces'})

    # Create new node, linked to the previous two
    db.create_node_with_relationships(labels="person", properties={"name": "Julian", "city": "Berkeley"},
                                      connections=[
                                          {"labels": "book",
                                           "key": "title", "value": "The Double Helix",
                                           "rel_name": "OWNS", "rel_dir": "OUT"},
                                          {"labels": "book",
                                           "key": "title", "value": "Intro to Hilbert Spaces",
                                           "rel_name": "OWNS", "rel_dir": "OUT"}
                                      ]
                                      )

    match = db.match(labels="person", properties={"name": "Julian", "city": "Berkeley"})

    number_links = db.count_links(match, rel_name="OWNS", rel_dir="OUT", neighbor_labels="book")

    assert number_links == 2



def test_get_parents_and_children(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    node = db.create_node("mid generation", {'age': 42, 'gender': 'F'})# This will be the "central node"
    node_hex_id=db.query("MATCH (n:`mid generation` {age: 42, gender: 'F'}) RETURN id(n) AS hex_id")[0].get('hex_id')
    result = db.get_parents_and_children(node_hex_id)
    assert result == ([], [])

    parent1 = db.create_node("parent", {'age': 62, 'gender': 'M'})
    parent1_hex_id=db.query("MATCH (n:parent {age: 62, gender: 'M'}) RETURN id(n) AS hex_id")[0].get("hex_id") # Add a first parent node
    db.link_nodes_by_ids(parent1_hex_id, node_hex_id, "PARENT_OF")

    result = db.get_parents_and_children(node_hex_id)

    assert result == ([{'internal_id': parent1_hex_id, 'labels': ['parent'], 'rel': 'PARENT_OF'}],
                      [])
    

    parent2 = db.create_node("parent", {'age': 52, 'gender': 'F'})  # Add 2nd parent
    parent2_hex_id=db.query("MATCH (n:parent {age: 52, gender: 'F'}) RETURN id(n) AS hex_id")[0].get("hex_id") # Add a first parent node
    db.link_nodes_by_ids(parent2_hex_id, node_hex_id, "PARENT_OF")

    (parent_list, child_list) = db.get_parents_and_children(node_hex_id)
    assert child_list == []
    compare_recordsets(parent_list,
                            [{'internal_id': parent1_hex_id, 'labels': ['parent'], 'rel': 'PARENT_OF'},
                             {'internal_id': parent2_hex_id, 'labels': ['parent'], 'rel': 'PARENT_OF'}
                            ]
                      )

    child1 = db.create_node("child", {'age': 13, 'gender': 'F'})  # Add a first child node
    child1_hex_id=db.query("MATCH (n:child {age: 13, gender: 'F'}) RETURN id(n) AS hex_id")[0].get("hex_id") # Add a first parent node

    db.link_nodes_by_ids(node_hex_id, child1_hex_id, "PARENT_OF")

    (parent_list, child_list) = db.get_parents_and_children(node_hex_id)
    assert child_list == [{'internal_id': child1_hex_id, 'labels': ['child'], 'rel': 'PARENT_OF'}]
    compare_recordsets(parent_list,
                            [{'internal_id': parent1_hex_id, 'labels': ['parent'], 'rel': 'PARENT_OF'},
                             {'internal_id': parent2_hex_id, 'labels': ['parent'], 'rel': 'PARENT_OF'}
                            ]
                      )

    child2 = db.create_node("child", {'age': 16, 'gender': 'F'})  # Add a 2nd child node
    child2_hex_id=db.query("MATCH (n:child {age: 16, gender: 'F'}) RETURN id(n) AS hex_id")[0].get("hex_id")
    db.link_nodes_by_ids(node_hex_id, child2_hex_id, "PARENT_OF")

    (parent_list, child_list) = db.get_parents_and_children(node_hex_id)
    compare_recordsets(child_list,
                            [{'internal_id': child1_hex_id, 'labels': ['child'], 'rel': 'PARENT_OF'},
                             {'internal_id': child2_hex_id, 'labels': ['child'], 'rel': 'PARENT_OF'}
                            ]
                      )
    compare_recordsets(parent_list,
                            [{'internal_id': parent1_hex_id, 'labels': ['parent'], 'rel': 'PARENT_OF'},
                             {'internal_id': parent2_hex_id, 'labels': ['parent'], 'rel': 'PARENT_OF'}
                            ]
                      )

    # Look at the children/parents of a "grandparent"
    result = db.get_parents_and_children(parent1_hex_id)
    assert result == ([],
                      [{'internal_id': node_hex_id, 'labels': ['mid generation'], 'rel': 'PARENT_OF'}]
                     )

    # Look at the children/parents of a "grandchild"
    result = db.get_parents_and_children(child2_hex_id)
    assert result == ([{'internal_id': node_hex_id, 'labels': ['mid generation'], 'rel': 'PARENT_OF'}],
                      []
                     )



def test_get_siblings(db):
    db.empty_dbase()

    # Create "French" and "German" nodes, as subcategory of "Language"
    q = '''
        CREATE (c1 :Categories {name: "French"})-[:subcategory_of]->(p :Categories {name: "Language"})<-[:subcategory_of]-
               (c2 :Categories {name: "German"})
        RETURN id(c1) AS french_id, id(c2) AS german_id, id(p) AS language_id
        '''
    create_result = db.query(q, single_row=True)

    (french_id, german_id, language_id) = list( map(create_result.get, ["french_id", "german_id", "language_id"]) )


    # The single sibling of "French" is "German"
    dt=db.get_siblings(internal_id=french_id, rel_name="subcategory_of", rel_dir="OUT")[0]
    dt.pop("internal_id",None)
    assert dt == {'name': 'German','neo4j_labels': ['Categories']}

#     # Conversely, the single sibling of "German" is "French"
    dt=db.get_siblings(internal_id=german_id, rel_name="subcategory_of", rel_dir="OUT")[0]
    dt.pop("internal_id",None)
    assert dt == {'name': 'French', 'neo4j_labels': ['Categories']}

    # But attempting to follow the links in the opposite directions will yield no results
    dt=db.get_siblings(internal_id=german_id, rel_name="subcategory_of", rel_dir="IN")   # "wrong" direction
    assert dt == []

    # Add a 3rd language category, "Italian", as a subcategory of the "Language" node
    italian_id = db.create_attached_node(labels="Categories", properties={"name": "Italian"},
                                         attached_to=language_id, rel_name="subcategory_of")

    # Now, "French" will have 2 siblings instead of 1
    result = db.get_siblings(internal_id=french_id, rel_name="subcategory_of", rel_dir="OUT")
    result = [{k: v for k, v in item.items() if k != 'internal_id'} for item in result]

    expected = [{'name': 'Italian','neo4j_labels': ['Categories']},
                {'name': 'German','neo4j_labels': ['Categories']}]
    assert compare_recordsets(result, expected)

    italian_id=db.get_node_internal_id("Categories",{"name": "Italian"})
    # # "Italian" will also have 2 siblings
    result = db.get_siblings(internal_id=italian_id, rel_name="subcategory_of", rel_dir="OUT")
    result = [{k: v for k, v in item.items() if k != 'internal_id'} for item in result]
    expected = [{'name': 'French', 'neo4j_labels': ['Categories']},
                {'name': 'German', 'neo4j_labels': ['Categories']}]
    assert compare_recordsets(result, expected)

    # Add a node that is a "parent" of "French" and "Italian" thru a different relationship
    db.create_attached_node(labels="Language_Family", properties={"name": "Romance"},
                            attached_to=[french_id, italian_id], rel_name="contains")

    # Now, "French" will also have a sibling thru the "contains" relationship
    result = db.get_siblings(internal_id=french_id, rel_name="contains", rel_dir="IN")
    result = [{k: v for k, v in item.items() if k != 'internal_id'} for item in result]
    expected = [{'name': 'Italian', 'neo4j_labels': ['Categories']}]
    assert compare_recordsets(result, expected)

    # Likewise for the "Italian" node
    result = db.get_siblings(internal_id=italian_id, rel_name="contains", rel_dir="IN")
    result = [{k: v for k, v in item.items() if k != 'internal_id'} for item in result]
    expected = [{'name': 'French', 'neo4j_labels': ['Categories']}]
    assert compare_recordsets(result, expected)

    # # "Italian" still has 2 siblings thru the other relationship "subcategory_of"
    result = db.get_siblings(internal_id=italian_id, rel_name="subcategory_of", rel_dir="OUT")
    result = [{k: v for k, v in item.items() if k != 'internal_id'} for item in result]
    expected = [{'name': 'French', 'neo4j_labels': ['Categories']},
                {'name': 'German', 'neo4j_labels': ['Categories']}]
    assert compare_recordsets(result, expected)

    # Add an unattached node
    brazilian_id = db.create_node(labels="Categories", properties={"name": "Brazilian"})
    brazilian_hex_id=db.get_node_internal_id(labels="Categories", properties={"name": "Brazilian"})
    result = db.get_siblings(internal_id=brazilian_hex_id, rel_name="subcategory_of", rel_dir="OUT")
    assert result == []     # No siblings

#     # After connecting the "Brazilian" node to the "Language" node, it has 3 siblings
    db.add_links(from_id=brazilian_hex_id, to_id=language_id, rel_name="subcategory_of")
    result = db.get_siblings(internal_id=brazilian_hex_id, rel_name="subcategory_of", rel_dir="OUT")
    result = [{k: v for k, v in item.items() if k != 'internal_id'} for item in result]

    expected = [{'name': 'French', 'neo4j_labels': ['Categories']},
                {'name': 'German','neo4j_labels': ['Categories']},
                {'name': 'Italian','neo4j_labels': ['Categories']}]
    assert compare_recordsets(result, expected)




# ###  ~ CREATE NODES ~

def test_create_node(db):
    """
    Test the trio:  1) clear dbase
                    2) create multiple new nodes (MAIN FOCUS)
                    3) retrieve the newly created nodes, using retrieve_nodes_by_label_and_clause()
    """

    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # Create a new node.  Notice the blank in the key
    db.create_node("test_label", {'patient id': 123, 'gender': 'M'})

    # Retrieve the record just created (one method, with values embedded in the Cypher query)
    match = db.match(labels="test_label", clause="n.`patient id` = 123 AND n.gender = 'M'")

    retrieved_records_A = db.get_nodes(match)
    assert retrieved_records_A == [{'patient id': 123, 'gender': 'M'}]


    # Create a second new node
    db.create_node("test_label", {'patient id': 123, 'gender': 'M', 'condition_id': 'happy'})

    # Retrieve cumulative 2 records created so far
    retrieved_records_B = db.get_nodes(match)

    # The lists defining the expected dataset can be expressed in any order - and, likewise, the order of entries in dictionaries doesn't matter
    expected_record_list = [{'patient id': 123, 'gender': 'M'} , {'patient id': 123, 'gender': 'M', 'condition_id': 'happy'}]
    expected_record_list_alt_order = [{'patient id': 123, 'gender': 'M', 'condition_id': 'happy'}  ,  {'gender': 'M', 'patient id': 123}]

    assert compare_recordsets(retrieved_records_B, expected_record_list)
    assert compare_recordsets(retrieved_records_B, expected_record_list_alt_order)  # We can test in any order :)


    # Create a 3rd node with a duplicate of the first new node
    db.create_node("test_label", {'patient id': 123, 'gender': 'M'})
    # Retrieve cumulative 3 records created so far
    retrieved_records_C = db.get_nodes(match)
    expected_record_list = [{'patient id': 123, 'gender': 'M'} ,
                            {'patient id': 123, 'gender': 'M'} ,
                            {'patient id': 123, 'gender': 'M', 'condition_id': 'happy'}]

    assert compare_recordsets(retrieved_records_C, expected_record_list)


    # Create a 4th node with no attributes, and a different label
    db.create_node("new_label", {})

    # Retrieve just this last node
    match = db.match(labels="new_label")
    retrieved_records_D = db.get_nodes(match)
    expected_record_list = [{}]
    assert compare_recordsets(retrieved_records_D, expected_record_list)


    # Create a 5th node with labels
    db.create_node(["label 1", "label 2"], {'name': "double label"})
    # Look it up by one label
    match = db.match(labels="label 1")
    retrieved_records = db.get_nodes(match)
    expected_record_list = [{'name': "double label"}]
    assert compare_recordsets(retrieved_records, expected_record_list)
    # Look it up by the other label
    match = db.match(labels="label 2")
    retrieved_records = db.get_nodes(match)
    expected_record_list = [{'name': "double label"}]
    assert compare_recordsets(retrieved_records, expected_record_list)
    # Look it up by both labels
    match = db.match(labels=["label 1", "label 2"])
    retrieved_records = db.get_nodes(match)
    expected_record_list = [{'name': "double label"}]
    assert compare_recordsets(retrieved_records, expected_record_list)



def test_create_node_with_relationships(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # Create the prior nodes to which to link the newly-created node
    db.create_node("DEPARTMENT", {'dept_name': 'IT'})
    db.create_node(["CAR", "INVENTORY"], {'vehicle_id': 12345})

    new_id = \
        db.create_node_with_relationships(
            labels="PERSON",
            properties={"name": "Julian", "city": "Berkeley"},
            connections=[
                {"labels": "DEPARTMENT",
                 "key": "dept_name", "value": "IT",
                 "rel_name": "EMPLOYS", "rel_dir": "IN"},

                {"labels": ["CAR", "INVENTORY"],
                 "key": "vehicle_id", "value": 12345,
                 "rel_name": "OWNS", "rel_attrs": {"since": 2021} }
            ]
        )
    print("ID of the newly-created node: ", new_id)

    q = '''
    MATCH (:DEPARTMENT {dept_name:'IT'})-[:EMPLOYS]
          ->(p:PERSON {name: 'Julian', city: 'Berkeley'})
          -[:OWNS {since:2021}]->(:CAR:INVENTORY {vehicle_id: 12345}) 
          RETURN id(p) AS internal_id
    '''
    result = db.query(q)
    print("result ------ ",result)
    assert result[0]['internal_id'] == new_id



def test_create_attached_node(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    node_jack = db.create_node("PERSON", {"name": "Jack"})
    node_jill = db.create_node("PERSON", {"name": "Jill"})
    node_mary = db.create_node("PERSON", {"name": "Mary"})

    jack_hex_id=db.get_node_internal_id("PERSON",{"name":"Jack"})
    jill_hex_id=db.get_node_internal_id("PERSON",{"name":"Jill"})
    mary_hex_id=db.get_node_internal_id("PERSON",{"name":"Mary"})
    # Create a new Company node and attach it to "Jack"
    new_node = db.create_attached_node(labels="COMPANY", attached_to=jack_hex_id, rel_name="EMPLOYS")
    company_hex_id=db.get_node_internal_id("COMPANY",{})
    q = f'''
        MATCH (c:COMPANY)-[:EMPLOYS]->(p:PERSON {{name: "Jack"}})
        WHERE id(c) = '{company_hex_id}'
        RETURN count(c) AS company_count, count(p) AS person_count
        '''
    result = db.query(q)
    assert result[0] == {'company_count': 1, 'person_count': 1}


    # Attach the already-created Company node to the 2 other people it employs ("Jill" and "Mary")
    new_node_2 = db.create_attached_node(labels="COMPANY", attached_to=[jill_hex_id, mary_hex_id], rel_name="EMPLOYS", merge=True)
    # new_node_2_id=new_node_2.get("internal_id")
    assert new_node_2 == new_node   # No new nodes created, because of the merge=True (and the already existing company node)

    q = f'''
        MATCH path=(c:COMPANY)-[:EMPLOYS]->(p:PERSON)
        WHERE id(c) = '{company_hex_id}' AND p.name IN ["Jack", "Jill", "Mary"]
        RETURN count(path) AS number_paths       
        '''
    result = db.query(q, single_cell="number_paths")
    assert result == 1

#     # TODO: test pathological scenarios where existing link-to nodes are mentioned multiple times



def test_create_node_with_links(db):

    db.empty_dbase(drop_indexes=False, drop_constraints=False)
    # Create a first node, with no links
    car_id = db.create_node_with_links(labels = ["CAR", "INVENTORY"],
                                       properties = {'vehicle_id': 12345})
    # Verify it
    node_count=db.count_nodes()
    assert node_count==1

#     # Create a 2nd node, also with no links
    dept_id = db.create_node_with_links(labels = ["DEPARTMENT"],
                                        properties = {'dept name': 'IT'})
#     # Verify it
    node_count=db.count_nodes()
    assert node_count==2


    # Create a new node linked to the 2 ones just created
    new_id = \
        db.create_node_with_links(
            labels="PERSON",
            properties={"name": "Julian", "city": "Berkeley"},
            links=[
                {"internal_id": dept_id,
                 "rel_name": "EMPLOYS",
                 "rel_dir": "IN"},

                {"internal_id": car_id,
                 "rel_name": "OWNS",
                 "rel_attrs": {"since": 2021} }
            ]
        )
    # print("ID of the newly-created node: ", new_id)

    q = '''
    MATCH (d:DEPARTMENT {`dept_name`: 'IT'})-[:EMPLOYS]->(p:PERSON {name: 'Julian', city: 'Berkeley'})-[:OWNS {since: 2021}]->(c:CAR:INVENTORY {vehicle_id: 12345}) RETURN id(p) AS internal_id


    '''
    db.query(q)
    # 
    db.create_node_with_links(
        labels="PERSON",
        properties={"name": "Val", "city": "San Francisco"},
        links=[
            {"internal_id": car_id,
                "rel_name": "DRIVES"},

            {"internal_id": car_id,
                "rel_name": "DRIVES"}
        ]
    )

    # In spite of the Exception, above, a new node was indeed created
    match = db.match(labels = "PERSON", properties={"name": "Val"})
    lookup = db.get_nodes(match)
    assert lookup == [{"name": "Val", "city": "San Francisco"}]



def test_assemble_query_for_linking(db):
    with pytest.raises(Exception):
        db._assemble_query_for_linking(None)
        db._assemble_query_for_linking("I'm not a list :(")
        db._assemble_query_for_linking([])

        db._assemble_query_for_linking([{}])
        db._assemble_query_for_linking([{'rel_name': 'OWNS'}])

        db._assemble_query_for_linking([{'internal_id': 'do I look like a number??'}])
        db._assemble_query_for_linking([{'internal_id': 123}])


    result = db._assemble_query_for_linking([{"internal_id": 123, "rel_name": "LIVES IN"}])
    assert result == ('MATCH (ex0)', 'WHERE id(ex0) = 123', 'MERGE (n)-[:`LIVES IN` ]->(ex0)', {})

    result = db._assemble_query_for_linking([{"internal_id": 456, "rel_name": "EMPLOYS", "rel_dir": "IN"}])
    assert result == ('MATCH (ex0)', 'WHERE id(ex0) = 456', 'MERGE (n)<-[:`EMPLOYS` ]-(ex0)', {})

    result = db._assemble_query_for_linking([{"internal_id": 789, "rel_name": "OWNS", "rel_attrs": {"since": 2022}}])
    assert result == ('MATCH (ex0)', 'WHERE id(ex0) = 789', 'MERGE (n)-[:`OWNS` {`since`: $EDGE0_1}]->(ex0)', {'EDGE0_1': 2022})


    result = db._assemble_query_for_linking([{"internal_id": 123, "rel_name": "LIVES IN"} ,
                                             {"internal_id": 456, "rel_name": "EMPLOYS", "rel_dir": "IN"}])
    assert result == ('MATCH (ex0), (ex1)',
                      'WHERE id(ex0) = 123 AND id(ex1) = 456',
                      'MERGE (n)-[:`LIVES IN` ]->(ex0)\nMERGE (n)<-[:`EMPLOYS` ]-(ex1)',
                      {}
                     )


    result = db._assemble_query_for_linking(
                        [
                            {"internal_id": 123, "rel_name": "LIVES IN"},
                            {"internal_id": 456, "rel_name": "EMPLOYS", "rel_dir": "IN"},
                            {"internal_id": 789, "rel_name": "IS OWNED BY", "rel_dir": "IN", "rel_attrs": {"since": 2022, "tax rate": "X 23"}}
                        ])
    assert result == (
                        'MATCH (ex0), (ex1), (ex2)',
                        'WHERE id(ex0) = 123 AND id(ex1) = 456 AND id(ex2) = 789',
                        'MERGE (n)-[:`LIVES IN` ]->(ex0)\nMERGE (n)<-[:`EMPLOYS` ]-(ex1)\nMERGE (n)<-[:`IS OWNED BY` {`since`: $EDGE2_1, `tax rate`: $EDGE2_2}]-(ex2)',
                        {'EDGE2_1': 2022, 'EDGE2_2': "X 23"}
                      )




# ###  ~ DELETE NODES ~

def test_delete_nodes_by_id(db):
    
    # TODO: No method available:
    
    # Clear the database before running the test
    db.empty_dbase(drop_indexes=False, drop_constraints=False)
  
    # Create some nodes to delete
    node1 = db.create_node("Person", {"name": "Alice"})

    node1_internal_id=db.get_node_internal_id("Person", {"name": "Alice"})
    # Get the internal IDs of the nodes
    node_ids = node1_internal_id

    # Delete the nodes
    num_deleted = db.delete_nodes_by_ids(node_ids)

    # Check if the correct number of nodes were deleted
    assert num_deleted == 1

    # Check if the nodes were actually deleted by querying the database
    result = db.query("MATCH (n) RETURN count(n) AS count")
    assert result[0]["count"] == 0

    print("All test cases passed successfully!")



def test_delete_nodes_by_label(db):
    db.delete_nodes_by_label()
    match = db.match()   # Everything in the dbase
    number_nodes = len(db.get_nodes(match))
    assert number_nodes == 0

    db.create_node("appetizers", {'name': 'spring roll'})
    db.create_node("vegetable", {'name': 'carrot'})
    db.create_node("vegetable", {'name': 'broccoli'})
    db.create_node("fruit", {'type': 'citrus'})
    db.create_node("dessert", {'name': 'chocolate'})

    assert len(db.get_nodes(match)) == 5

    db.delete_nodes_by_label(delete_labels="fruit")
    assert len(db.get_nodes(match)) == 4

    db.delete_nodes_by_label(delete_labels=["vegetable"])
    assert len(db.get_nodes(match)) == 2

    db.delete_nodes_by_label(delete_labels=["dessert", "appetizers"])
    assert len(db.get_nodes(match)) == 0

    # Rebuild the same dataset as before
    db.create_node("appetizers", {'name': 'spring roll'})
    db.create_node("vegetable", {'name': 'carrot'})
    db.create_node("vegetable", {'name': 'broccoli'})
    db.create_node("fruit", {'type': 'citrus'})
    db.create_node("dessert", {'name': 'chocolate'})

    db.delete_nodes_by_label(keep_labels=["dessert", "vegetable", "appetizers"])
    assert len(db.get_nodes(match)) == 4

    db.delete_nodes_by_label(keep_labels="dessert", delete_labels="dessert")
    # Keep has priority over delete
    assert len(db.get_nodes(match)) == 4

    db.delete_nodes_by_label(keep_labels="dessert")
    assert len(db.get_nodes(match)) == 1



def test_empty_dbase(db):
    # Tests of completely clearing the database

    db.empty_dbase(drop_indexes=False, drop_constraints=False)
    # Verify nothing is left
    labels = db.get_labels()
    assert labels == []

    db.create_node("label_A", {})
    db.create_node("label_B", {'client_id': 123, 'gender': 'M'})

    db.empty_dbase(drop_indexes=False, drop_constraints=False)
    # Verify nothing is left
    labels = db.get_labels()
    assert labels == []

    # Test of removing only specific labels

    db.empty_dbase(drop_indexes=False, drop_constraints=False)
    # Add a few labels
    db.create_node("label_1", {'client_id': 123, 'gender': 'M'})
    db.create_node("label_2", {})
    db.create_node("label_3", {'client_id': 456, 'name': 'Julian'})
    db.create_node("label_4", {})
    # Only clear the specified labels
    db.delete_nodes_by_label(delete_labels=["label_1", "label_4"])
    # Verify that only labels not marked for deletions are left behind
    labels = db.get_labels()
    assert compare_unordered_lists(labels , ["label_2", "label_3"])

    # Test of keeping only specific labels

    db.empty_dbase(drop_indexes=False, drop_constraints=False)
    # Add a few labels
    db.create_node("label_1", {'client_id': 123, 'gender': 'M'})
    db.create_node("label_2", {})
    db.create_node("label_3", {'client_id': 456, 'name': 'Julian'})
    db.create_node("label_4", {})
    # Only keep the specified labels
    db.empty_dbase(keep_labels=["label_4", "label_3"])
    # Verify that only labels not marked for deletions are left behind
    labels = db.get_labels()
    assert compare_unordered_lists(labels , ["label_4", "label_3"])
    # Doubly-verify that one of the saved nodes can be read in
    match = db.match(labels="label_3")
    recordset = db.get_nodes(match)
    assert compare_recordsets(recordset, [{'client_id': 456, 'name': 'Julian'}])




###  ~ MODIFY FIELDS ~

def test_set_fields(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # Create a new node.  Notice the blank in the key
    db.create_node("car", {'vehicle id': 123, 'price': 7500})

    # Locate the node just created, and create/update its attributes (reduce the price)
    match = db.match(labels="car", properties={'vehicle id': 123, 'price': 7500})
    db.set_fields(match=match, set_dict = {"color": "white", "price": 7000})

    # Look up the updated record
    match = db.match(labels="car")
    retrieved_records = db.get_nodes(match)
    expected_record_list = [{'vehicle id': 123, 'color': 'white', 'price': 7000}]
    assert compare_recordsets(retrieved_records, expected_record_list)




###  ~ RELATIONSHIPS ~

# def test_get_relationship_types(db):
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)
#     rels = db.get_relationship_types()
#     assert rels == []

#     node1_id = db.create_node("Person", {'p_id': 1})
#     node2_id = db.create_node("Person", {'p_id': 2})
#     node3_id = db.create_node("Person", {'p_id': 3})
#     db.link_nodes_by_ids(node1_id, node2_id, "LOVES")
#     db.link_nodes_by_ids(node2_id, node3_id, "HATES")

#     rels = db.get_relationship_types()
#     assert set(rels) == {"LOVES", "HATES"}



def test_add_links(db):
    #todo: the update_query in neptune does not return the metadata data as of neo4j..so we cannot test this one
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    neo_from = db.create_node("car", {'color': 'white'})
    neo_to = db.create_node("owner", {'name': 'Julian'})

    neo_from_hex_id=db.get_node_internal_id("car", {'color': 'white'})
    neo_to_hex_id=db.get_node_internal_id("owner", {'name': 'Julian'})

    number_added = db.add_links(from_id=neo_from_hex_id, to_id=neo_to_hex_id, rel_name="OWNED_BY")
    assert number_added[0].get("link_count") == 1

    q = '''
        MATCH (c:car)-[:OWNED_BY]->(o:owner) 
        RETURN count(c) AS number_cars, count(o) AS number_owners
        '''
    result = db.query(q, single_row=True)

    assert result == {'number_cars': 1, 'number_owners': 1}


    # Make a link from the "car" node to itself
    assert db.add_links(neo_from_hex_id, neo_from_hex_id, rel_name="FROM_CAR_TO_ITSELF") # Note the blanks in the name
    assert number_added[0].get("link_count") == 1

    q = '''MATCH (c:car)-[:FROM_CAR_TO_ITSELF]->(c) 
        RETURN count(c) AS number_cars'''
    result = db.query(q, single_row=True)
    assert result == {'number_cars': 1}


def test_remove_edges(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    neo_car = db.create_node("car", {'color': 'white'})
    neo_julian = db.create_node("owner", {'name': 'Julian'})

    neo_car_hex_id=db.get_node_internal_id("car", {'color': 'white'})
    neo_julian_hex_id=db.get_node_internal_id("owner", {'name': 'Julian'})

    number_added = db.add_links(neo_car_hex_id, neo_julian_hex_id, rel_name="OWNED_BY")[0].get("link_count")
    assert number_added == 1

    
    match_car = db.match(labels="car", dummy_node_name="from")
    match_julian = db.match(properties={"name": "Julian"}, dummy_node_name="to")

    find_query = '''
        MATCH (c:car)-[:OWNED_BY]->(o:owner) 
        RETURN count(c) As n_cars
    '''
    result = db.query(find_query)
    assert result[0]["n_cars"] == 1     # Find the relationship

    # Finally, actually remove the edge
    number_removed = db.remove_links(neo_car_hex_id, neo_julian_hex_id, rel_name="OWNED_BY")
    assert number_removed[0].get("deleted_count") == 1

    result = db.query(find_query)
    assert result[0]["n_cars"] == 0     # The relationship is now gone

    # Restore the relationship...
    number_added = db.add_links(neo_car_hex_id, neo_julian_hex_id, rel_name="OWNED_BY")
    assert number_added[0].get("link_count") == 1

    # # ...and add a 2nd one, with a different name, between the same nodes
    number_added = db.add_links(neo_car_hex_id, neo_julian_hex_id, rel_name="REMEMBERED_BY")
    assert number_added[0].get("link_count") == 1

    # # ...and re-add the last one, but with a property (which is allowed by Neo4j, and will result in
    # #       2 relationships with the same name between the same node
    add_query = '''
        MATCH (c:car), (o:owner)
        MERGE (c)-[:REMEMBERED_BY {since: 2020}]->(o)
    '''
    db.query(add_query)
    # assert result == {'_contains_updates': True, 'relationships_created': 1, 'properties_set': 1, 'returned_data': []}
    # # Note: '_contains_updates': True  was added in version 4.4 of Neo4j

    # # Also, add a 3rd node, and another "OWNED_BY" relationship, this time affecting the 3rd node
    add_query = '''
        MATCH (c:car)
        MERGE (c)-[:OWNED_BY]->(o :owner {name: 'Val'})
    '''
    db.query(add_query)
    # # We now have a car with 2 owners: an "OWNED_BY" relationship to one of them,
    # # and 3 relationships (incl. two with the same name "REMEMBERED_BY") to the other one

    find_query = '''
        MATCH (c:car)-[:REMEMBERED_BY]->(o:owner ) 
        RETURN count(c) As n_cars
    '''
    result = db.query(find_query)
    assert result[0]["n_cars"] == 2     # The 2 relationships we just added

    # Remove 2 same-named relationships at once between the same 2 nodes
    number_removed = db.remove_links(neo_car_hex_id, neo_julian_hex_id, rel_name="REMEMBERED_BY")
    assert number_removed[0].get("deleted_count") == 2

    result = db.query(find_query)
    assert result[0]["n_cars"] == 0     # Gone

    find_query = '''
        MATCH (c:car)-[:OWNED_BY]->(o:owner {name: 'Julian'}) 
        RETURN count(c) As n_cars
    '''
    result = db.query(find_query)
    assert result[0]["n_cars"] == 1     # Still there

    find_query = '''
        MATCH (c:car)-[:OWNED_BY]->(o:owner {name: 'Val'}) 
        RETURN count(c) As n_cars
    '''
    result = db.query(find_query)
    assert result[0]["n_cars"] == 1     # Still there


    number_removed = db.remove_links(neo_car_hex_id, neo_julian_hex_id, rel_name="OWNED_BY")
    assert number_removed[0].get("deleted_count") == 1

    find_query = '''
        MATCH (c:car)-[:OWNED_BY]->(o:owner {name: 'Julian'}) 
        RETURN count(c) As n_cars
    '''
    result = db.query(find_query)
    assert result[0]["n_cars"] == 0     # Gone

    find_query = '''
        MATCH (c:car)-[:OWNED_BY]->(o:owner {name: 'Val'}) 
        RETURN count(c) As n_cars
    '''
    result = db.query(find_query)
    assert result[0]["n_cars"] == 1     # We didn't do anything to that relationship

    # Add a 2nd relations between the car node and the "Val" owner node
    add_query = '''
        MATCH (c:car), (o :owner {name: 'Val'})
        MERGE (c)-[:DRIVEN_BY]->(o)
        '''
    db.query(add_query)
    
    find_query = '''
        MATCH (c:car)-[r]->(o:owner {name: 'Val'}) 
        RETURN count(r) As n_relationships
    '''
    result = db.query(find_query)
    assert result[0]["n_relationships"] == 2

    # Delete both relationships at once
    neo_val_hex_id=db.get_node_internal_id("owner", {'name': 'Val'})


    number_removed = db.remove_links(neo_car_hex_id, neo_val_hex_id, rel_name='OWNED_BY')
    assert number_removed[0].get("deleted_count") == 1

    result = db.query(find_query)
    assert result[0]["n_relationships"] == 1



def test_edges_exists(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    neo_car = db.create_node("car", {'color': 'white'})
    neo_julian = db.create_node("owner", {'name': 'Julian'})

    neo_car_hex_id=db.get_node_internal_id("car", {'color': 'white'})
    neo_julian_hex_id=db.get_node_internal_id("owner", {'name': 'Julian'})

    assert not db.links_exist(neo_car_hex_id, neo_julian_hex_id, rel_name="OWNED_BY")    # No relationship exists yet

    db.add_links(neo_car_hex_id, neo_julian_hex_id, rel_name="OWNED_BY")
    assert db.links_exist(neo_car_hex_id, neo_julian_hex_id, rel_name="OWNED_BY")        # By now, it exists
    assert not db.links_exist(neo_julian_hex_id, neo_car_hex_id, rel_name="OWNED_BY")    # But not in the reverse direction
    assert not db.links_exist(neo_car_hex_id, neo_julian_hex_id, rel_name="DRIVEN_BY")   # Nor by a different name

    db.add_links(neo_car_hex_id, neo_julian_hex_id, rel_name="DRIVEN_BY")
    assert db.links_exist(neo_car_hex_id, neo_julian_hex_id, rel_name="DRIVEN_BY")       # Now it exists

    # todo: test when remove edges ready
    # db.remove_links(neo_car_hex_id, neo_julian_hex_id, rel_name="DRIVEN_BY")
    # assert not db.links_exist(neo_car_hex_id, neo_julian_hex_id, rel_name="DRIVEN_BY")   # Now it's gone

    neo_sailboat = db.create_node("sailboat", {'type': 'sloop', 'color': 'white'})
    neo_sailboat_hex_id=db.get_node_internal_id("sailboat", {'type': 'sloop', 'color': 'white'})

    db.add_links(neo_julian_hex_id, neo_sailboat_hex_id, rel_name="SAILS")
    assert db.links_exist(neo_julian_hex_id, neo_sailboat_hex_id, rel_name="SAILS")

    assert not db.links_exist(neo_car_hex_id, neo_car_hex_id, rel_name="SELF_DRIVES")     # A relationship from a node to itself
    db.add_links(neo_car_hex_id, neo_car_hex_id, rel_name="SELF_DRIVES")
    assert db.links_exist(neo_car_hex_id, neo_car_hex_id, rel_name="SELF_DRIVES")



def test_number_of_links(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    neo_car = db.create_node("car", {'color': 'white'})
    neo_julian = db.create_node("owner", {'name': 'Julian'})

    neo_car_hex_id=db.get_node_internal_id("car", {'color': 'white'})
    neo_julian_hex_id=db.get_node_internal_id("owner", {'name': 'Julian'})

    assert db.number_of_links(neo_car_hex_id, neo_julian_hex_id, rel_name="OWNED_BY") == 0    # No relationship exists yet

    db.add_links(neo_car_hex_id, neo_julian_hex_id, rel_name="OWNED_BY")
    assert db.number_of_links(neo_car_hex_id, neo_julian_hex_id, rel_name="OWNED_BY") == 1    # By now, it exists
    assert db.number_of_links(neo_julian_hex_id, neo_car_hex_id, rel_name="OWNED_BY") == 0    # But not in the reverse direction
    assert db.number_of_links(neo_car_hex_id, neo_julian_hex_id, rel_name="DRIVEN_BY") == 0   # Nor by a different name

    db.add_links(neo_car_hex_id, neo_julian_hex_id, rel_name="DRIVEN_BY")
    assert db.number_of_links(neo_car_hex_id, neo_julian_hex_id, rel_name="DRIVEN_BY") == 1   # Now it exists with this other name

    #todo test this after remove_links method
    # db.remove_links(neo_car_hex_id, neo_julian_hex_id, rel_name="DRIVEN BY")
    # assert db.number_of_links(neo_car_hex_id, neo_julian_hex_id, rel_name="DRIVEN_BY") == 0   # Now it's gone

    neo_sailboat = db.create_node("sailboat", {'type': 'sloop', 'color': 'white'})
    neo_sailboat_hex_id=db.get_node_internal_id("sailboat", {'type': 'sloop', 'color': 'white'})

    db.add_links(neo_julian_hex_id, neo_sailboat_hex_id, rel_name="SAILS")
    assert db.number_of_links(neo_julian_hex_id, neo_sailboat_hex_id, rel_name="SAILS") == 1  # It finds the just-added link

    assert db.number_of_links(neo_car_hex_id, neo_car_hex_id, rel_name="SELF_DRIVES") == 0    # A relationship from a node to itself
    db.add_links(neo_car_hex_id, neo_car_hex_id, rel_name="SELF_DRIVES")
    assert db.number_of_links(neo_car_hex_id, neo_car_hex_id, rel_name="SELF_DRIVES") == 1    # Find the link to itself



def test_get_node_internal_id(db):
    

    db.empty_dbase()

    match_all = db.match()              # Meaning "match everything"

    with pytest.raises(Exception):
        db.get_node_internal_id(match_all)  # No nodes yet exist

    # Add a 1st node
    adam_id = db.create_node(labels="Person", properties={"name": "Adam"})
    internal_id = db.get_node_internal_id(labels="Person", properties={"name": "Adam"}) 
    adam_hex_id=db.query("MATCH (n:Person {name: 'Adam'}) RETURN id(n) AS INTERNAL_ID")[0].get("INTERNAL_ID")    # Finds all nodes (there's only 1 in the database)
    # Finds all nodes (there's only 1 in the database)
    assert internal_id == adam_hex_id

    # Add a 2nd node
    eve_id = db.create_node(labels="Person", properties={"name": "Eve"})
    eve_hex_id=db.query("MATCH (n:Person {name: 'Eve'}) RETURN id(n) AS INTERNAL_ID")[0] 

    with pytest.raises(Exception):
        db.get_node_internal_id(match_all)          # It finds 2 nodes - a non-unique result

    match_adam = db.match(properties={"name": "Adam"})
    internal_id = db.get_node_internal_id(properties={"name":"Adam"})     # Finds the "Adam" node
    assert internal_id == adam_hex_id

def test_reattach_node(db):
    db.empty_dbase()

    jack = db.create_node("Person", {"name": "Jack"})
    jack_hex_id=db.get_node_internal_id("Person", {"name": "Jack"})

    jill = db.create_attached_node("Person", properties={"name": "Jill"},
                            attached_to=jack_hex_id, rel_name="MARRIED_TO", rel_dir="IN")
    mary = db.create_node("Person", {"name": "Mary"})

    jill_hex_id=db.get_node_internal_id("Person", properties={"name": "Jill"})
    mary_hex_id=db.get_node_internal_id("Person", {"name": "Mary"})

    # jack finally shakes off jill and marries mary
    db.reattach_node(node=jack_hex_id, old_attachment=jill_hex_id, new_attachment=mary_hex_id, rel_name="MARRIED_TO")

    q = '''
    MATCH (n1:Person)-[:MARRIED_TO]->(n2:Person) RETURN id(n1) AS ID_FROM, id(n2) AS ID_TO
    '''
    result = db.query(q)
    assert len(result) == 1
    assert result[0] == {"ID_FROM": jack_hex_id, "ID_TO": mary_hex_id}

    # Let's eliminate the "jill" node
    db.delete_nodes_by_ids(jill_hex_id)

    # Let's re-introduce a "jill" node
    jill_2 = db.create_node("Person", {"name": "Jill"})
    jill_2_hex_id=db.get_node_internal_id("Person", {"name": "Jill"})

    # Now the indecisive jack can go back to jill (the new node with internal ID stored in jill_2)
    db.reattach_node(node=jack_hex_id, old_attachment=mary_hex_id, new_attachment=jill_2_hex_id, rel_name="MARRIED_TO")

    q = '''
    MATCH (n1:Person)-[:MARRIED_TO]->(n2:Person) RETURN id(n1) AS ID_FROM, id(n2) AS ID_TO'''
    result = db.query(q)
    assert len(result) == 1
    assert result[0] == {"ID_FROM": jack_hex_id, "ID_TO": jill_2_hex_id}



def test_link_nodes_by_ids(db):
    # Clear the database
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # Create dummy nodes
    node1 = db.create_node('A', {'client': 'GSK', 'expenses': 34000, 'duration': 3})
    node2 = db.create_node('B', {'client': 'Roche'})
    node3 = db.create_node('C', {'client': 'GSK'})
    
    node1_hex_id=db.query("MATCH (n:A {client: 'GSK', expenses: 34000, duration: 3}) RETURN id(n) AS hex_id")[0].get('hex_id')
    node2_hex_id=db.query("MATCH (n:B {client: 'Roche'}) RETURN id(n) AS hex_id")[0].get('hex_id')
    node3_hex_id=db.query("MATCH (n:C {client: 'GSK'}) RETURN id(n) AS hex_id")[0].get('hex_id')

    # Link the first and second nodes
    test_rel_props = {'test 1': 123, 'TEST2': 'abc'}
    db.link_nodes_by_ids(node1_hex_id,node2_hex_id, 'TEST_REL', test_rel_props)

    # Query the database to check if the relationship was created
    result = db.query("""
        MATCH (a)-[r:TEST_REL]->(b)
        RETURN a,count(r),b
    """)

#     # Assert that the query result is not empty and contains the expected nodes and relationship
    assert len(result) == 1
    assert result[0]['a']["client"] == 'GSK'
    assert result[0]['b']["client"] == 'Roche'



def test_link_nodes_on_matching_property(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)
    db.create_node('A', {'client': 'GSK', 'expenses': 34000, 'duration': 3})
    db.create_node('B', {'client': 'Roche'})
    db.create_node('C', {'client': 'GSK'})
    db.create_node('B', {'client': 'GSK'})
    db.create_node('C', {'client': 'Pfizer', 'revenues': 34000})

    db.link_nodes_on_matching_property("A", "B", "client", rel="SAME_CLIENT")
    q = "MATCH(a:A)-[SAME_AGE]->(b:B) RETURN a, b"
    res = db.query(q)
    assert len(res) == 1

    record = res[0]
    assert record["a"] == {'client': 'GSK', 'expenses': 34000, 'duration': 3}
    assert record["b"] == {'client': 'GSK'}

    db.link_nodes_on_matching_property("A", "C", property1="expenses", property2="revenues", rel="MATCHED_BUDGET")
    q = "MATCH(a:A)-[MATCHED_BUDGET]->(b:B) RETURN a, b"
    res = db.query(q)
    assert len(res) == 1




##  ~ LABELS ~

def test_get_labels(db):
    """
    Create multiple new nodes, and then retrieve all the labels present in the database
    """

    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    labels = db.get_labels()
    assert labels == []

    # Create a series of new nodes with different labels
    # and then check the cumulative list of labels added to the dbase thus far

    db.create_node("mercury", {'position': 1})
    labels = db.get_labels()
    assert labels == ["mercury"]

    db.create_node("venus", {'radius': 1234.5})
    labels = db.get_labels()
    assert compare_unordered_lists(labels , ["mercury", "venus"])

    db.create_node("earth", {'mass': 9999.9 , 'radius': 1234.5})
    labels = db.get_labels()
    assert compare_unordered_lists(labels , ["mercury", "earth", "venus"]) # The expected list may be
                                                                            # specified in any order

    db.create_node("mars", {})
    labels=db.get_labels()
    assert compare_unordered_lists(labels , ["mars", "earth", "mercury","venus"])



def test_get_label_properties(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)
    db.query("CREATE (a1 :A {y:123}), (a2 :A {x:'hello'}), (a3 :A {`some name with blanks`:'x'}), (b :B {e:1})")
    result = db.get_label_properties(label = 'A')
    expected_result = ['some name with blanks', 'x', 'y']
    assert result == expected_result




# ##  ~ INDEXES ~

# def test_get_indexes(db):
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     result = db.get_indexes()
#     assert result.empty

#     db.query("CREATE INDEX FOR (n:my_label) ON (n.my_property)")
#     result = db.get_indexes()
#     assert result.iloc[0]["labelsOrTypes"] == ["my_label"]
#     assert result.iloc[0]["properties"] == ["my_property"]
#     assert result.iloc[0]["type"] == "BTREE"
#     assert result.iloc[0]["uniqueness"] == "NONUNIQUE"

#     db.query("CREATE CONSTRAINT some_name ON (n:my_label) ASSERT n.node_id IS UNIQUE")
#     result = db.get_indexes()
#     new_row = dict(result.iloc[1])
#     assert new_row == {"labelsOrTypes": ["my_label"],
#                        "name": "some_name",
#                        "properties": ["node_id"],
#                        "type": "BTREE",
#                        "uniqueness": "UNIQUE"
#                       }



# def test_create_index(db):
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     status = db.create_index(label="car", key="color")
#     assert status == True

#     result = db.get_indexes()
#     assert len(result) == 1
#     assert result.iloc[0]["labelsOrTypes"] == ["car"]
#     assert result.iloc[0]["name"] == "car.color"
#     assert result.iloc[0]["properties"] == ["color"]
#     assert result.iloc[0]["type"] == "BTREE"
#     assert result.iloc[0]["uniqueness"] == "NONUNIQUE"

#     status = db.create_index("car", "color")    # Attempt to create again same index
#     assert status == False

#     status = db.create_index("car", "make")
#     assert status == True

#     result = db.get_indexes()
#     assert len(result) == 2
#     assert result.iloc[1]["labelsOrTypes"] == ["car"]
#     assert result.iloc[1]["name"] == "car.make"
#     assert result.iloc[1]["properties"] == ["make"]
#     assert result.iloc[1]["type"] == "BTREE"
#     assert result.iloc[1]["uniqueness"] == "NONUNIQUE"



# def test_drop_index(db):
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     db.create_index("car", "color")
#     db.create_index("car", "make")
#     db.create_index("vehicle", "year")
#     db.create_index("vehicle", "factory")

#     index_df = db.get_indexes()
#     assert len(index_df) == 4

#     status = db.drop_index("car.make")
#     assert status == True
#     index_df = db.get_indexes()
#     assert len(index_df) == 3

#     status = db.drop_index("car.make")  # Attempt to take out an index that is not present
#     assert status == False
#     index_df = db.get_indexes()
#     assert len(index_df) == 3


# def test_drop_all_indexes(db):
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     db.create_index("car", "color")
#     db.create_index("car", "make")
#     db.create_index("vehicle", "year")

#     index_df = db.get_indexes()
#     assert len(index_df) == 3

#     db.drop_all_indexes()

#     index_df = db.get_indexes()
#     assert len(index_df) == 0




# # ###  ~ CONSTRAINTS ~

# def test_get_constraints(db):
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     result = db.get_constraints()
#     assert result.empty

#     db.query("CREATE CONSTRAINT my_first_constraint ON (n:patient) ASSERT n.patient_id IS UNIQUE")
#     result = db.get_constraints()
#     assert len(result) == 1
#     expected_list = ["name", "description", "details"]
#     compare_unordered_lists(list(result.columns), expected_list)
#     assert result.iloc[0]["name"] == "my_first_constraint"

#     db.query("CREATE CONSTRAINT unique_model ON (n:car) ASSERT n.model IS UNIQUE")
#     result = db.get_constraints()
#     assert len(result) == 2
#     expected_list = ["name", "description", "details"]
#     compare_unordered_lists(list(result.columns), expected_list)
#     assert result.iloc[1]["name"] == "unique_model"



# def test_create_constraint(db):
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     status = db.create_constraint("patient", "patient_id", name="my_first_constraint")
#     assert status == True

#     result = db.get_constraints()
#     assert len(result) == 1
#     expected_list = ["name", "description", "details"]
#     compare_unordered_lists(list(result.columns), expected_list)
#     assert result.iloc[0]["name"] == "my_first_constraint"

#     status = db.create_constraint("car", "registration_number")
#     assert status == True

#     result = db.get_constraints()
#     assert len(result) == 2
#     expected_list = ["name", "description", "details"]
#     compare_unordered_lists(list(result.columns), expected_list)
#     cname0 = result.iloc[0]["name"]
#     cname1 = result.iloc[1]["name"]
#     assert cname0 == "car.registration_number.UNIQUE" or cname1 == "car.registration_number.UNIQUE"

#     status = db.create_constraint("car", "registration_number")   # Attempt to create a constraint that already was in place
#     assert status == False
#     result = db.get_constraints()
#     assert len(result) == 2

#     db.create_index("car", "parking_spot")

#     status = db.create_constraint("car", "parking_spot")    # Attempt to create a constraint for which there was already an index
#     assert status == False
#     result = db.get_constraints()
#     assert len(result) == 2



# def test_drop_constraint(db):
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     db.create_constraint("patient", "patient_id", name="constraint1")
#     db.create_constraint("client", "client_id")

#     result = db.get_constraints()
#     assert len(result) == 2

#     status = db.drop_constraint("constraint1")
#     assert status == True
#     result = db.get_constraints()
#     assert len(result) == 1

#     status = db.drop_constraint("constraint1")  # Attempt to remove a constraint that doesn't exist
#     assert status == False
#     result = db.get_constraints()
#     assert len(result) == 1

#     status = db.drop_constraint("client.client_id.UNIQUE")  # Using the name automatically assigned by create_constraint()
#     assert status == True
#     result = db.get_constraints()
#     assert len(result) == 0



# def test_drop_all_constraints(db):
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     db.create_constraint("patient", "patient_id", name="constraint1")
#     db.create_constraint("client", "client_id")

#     result = db.get_constraints()
#     assert len(result) == 2

#     db.drop_all_constraints()

#     result = db.get_constraints()
#     assert len(result) == 0




###  ~ READ IN DATA from PANDAS ~

def test_load_pandas_1(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # Start with a single imported node
    df = pd.DataFrame([[123]], columns = ["col1"])  # One row, one column
    db.load_pandas(df, labels="A", ignore_nan=False)
    match_A = db.match(labels="A")  # To pull all nodes with a label "A"
    result = db.get_nodes(match_A)
    assert result == [{'col1': 123}]

    # Append a new single node
    df = pd.DataFrame([[999]], columns = ["col1"])
    db.load_pandas(df, labels="A", ignore_nan=True)
    result = db.get_nodes(match_A)
    expected = [{'col1': 123}, {'col1': 999}]
    assert compare_recordsets(result, expected)

    # Append a new single node
    df = pd.DataFrame([[2222]], columns = ["col2"])
    db.load_pandas(df, labels="A")
    result = db.get_nodes(match_A)
    expected = [{'col1': 123}, {'col1': 999}, {'col2': 2222}]
    assert compare_recordsets(result, expected)

    # Append a new single node
    df = pd.DataFrame([[3333]], columns = ["col3"])
    db.load_pandas(df, labels="B")
    A_nodes = db.get_nodes(match_A)
    expected_A = [{'col1': 123}, {'col1': 999}, {'col2': 2222}]
    assert compare_recordsets(A_nodes, expected_A)
    match_B = db.match(labels="B")  # To pull all nodes with a label "B"
    B_nodes = db.get_nodes(match_B)
    assert B_nodes == [{'col3': 3333}]


    db.load_pandas(df, labels="B", merge_primary_key=None)    # Re-add the same identical record
    B_nodes = db.get_nodes(match_B)
    assert B_nodes == [{'col3': 3333}, {'col3': 3333}]

    # Add a 2x2 dataframe
    df = pd.DataFrame({"col3": [100, 200], "name": ["Jack", "Jill"]})
    db.load_pandas(df, labels="A")
    A_nodes = db.get_nodes(match_A)
    expected = [{'col1': 123}, {'col1': 999}, {'col2': 2222}, {'col3': 100, 'name': 'Jack'}, {'col3': 200, 'name': 'Jill'}]
    assert compare_recordsets(A_nodes, expected)

    # Change the column names during import
    df = pd.DataFrame({"alternate_name": [1000]})
    db.load_pandas(df, labels="B", rename={"alternate_name": "col3"})     # Map "alternate_name" into "col3"
    B_nodes = db.get_nodes(match_B)
    expected_B = [{'col3': 3333}, {'col3': 3333}, {'col3': 1000}]
    assert compare_recordsets(B_nodes, expected_B)

    # Add 2 more records, with double labels
    df = pd.DataFrame({"patient_id": [100, 200], "name": ["Jack", "Jill"]})
    db.load_pandas(df, labels=["X", "Y"])
    match_X_Y = db.match(labels=["X", "Y"])
    X_Y_nodes = db.get_nodes(match_X_Y)
    expected_X_Y = [{'patient_id': 100, 'name': 'Jack'}, {'patient_id': 200, 'name': 'Jill'}]
    assert compare_recordsets(X_Y_nodes, expected_X_Y)


# def test_load_pandas_2(db):
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     # Add 3 records
#     df = pd.DataFrame({"scientist_id": [10, 20, 30], "name": ["Julian", "Jack", "Jill"], "location": ["CA", "NY", "DC"]})
#     id_list = db.load_pandas(df, labels="scientist", merge_primary_key=None)

#     q = f'''
#         MATCH (n :scientist) 
#         WHERE id(n) IN {id_list}
#         RETURN n
#         '''
#     res = db.query(q, single_column="n")
#     expected = [{'name': 'Julian', 'location': 'CA', 'scientist_id': 10},
#                 {'name': 'Jack', 'location': 'NY', 'scientist_id': 20},
#                 {'name': 'Jill', 'location': 'DC', 'scientist_id': 30}]
#     assert compare_recordsets(res, expected)

#     # Update the "Julian" node, indexed by scientist_id (we'll modify the "name" of that node, and add a "specialty" field;
#     # notice that the "location" field doesn't get altered)
#     df = pd.DataFrame({"scientist_id": [10], "name": ["Julian W"], "specialty": ["Systems Biology"]})
#     db.load_pandas(df, labels="scientist", merge_primary_key="scientist_id", merge_overwrite=False)
#     q = "MATCH (n :scientist) RETURN n"
#     res = db.query(q, single_column="n")
#     expected = [{'specialty': 'Systems Biology', 'name': 'Julian W', 'location': 'CA', 'scientist_id': 10},
#                 {'name': 'Jack', 'location': 'NY', 'scientist_id': 20},
#                 {'name': 'Jill', 'location': 'DC', 'scientist_id': 30}]
#     assert compare_recordsets(res, expected)

#     # This time, completely replace the "Julian" node, indexed by scientist_id.
#     # Notice how all the previous fields that aren't being set now, are gone ("specialty" and "location")
#     df = pd.DataFrame({"scientist_id": [10], "name": ["Jules"]})
#     db.load_pandas(df, labels="scientist", merge_primary_key="scientist_id", merge_overwrite=True)
#     q = "MATCH (n :scientist) RETURN n"
#     res = db.query(q, single_column="n")
#     expected = [{'name': 'Jules', 'scientist_id': 10},
#                 {'name': 'Jack', 'location': 'NY', 'scientist_id': 20},
#                 {'name': 'Jill', 'location': 'DC', 'scientist_id': 30}]
#     assert compare_recordsets(res, expected)


#     # Verify that a database Index got created as a result of using merge_primary_key
#     all_indexes = db.get_indexes()
#     assert len(all_indexes) == 1
#     index_as_list = list(all_indexes.iloc[0])
#     assert index_as_list == [['scientist'], 'scientist.scientist_id', ['scientist_id'], 'BTREE', 'NONUNIQUE']


#     # More tests of merge with primary_key
#     df = pd.DataFrame({"patient_id": [100, 200], "name": ["Adam", "Eve"], "age": [21, 19]})
#     db.load_pandas(df, labels="X")
#     match_X = db.match(labels=["X"])
#     X_nodes = db.get_nodes(match_X)
#     expected = [{'patient_id': 100, 'name': 'Adam', 'age': 21}, {'patient_id': 200, 'name': 'Eve', 'age': 19}]
#     assert compare_recordsets(X_nodes, expected)

#     df = pd.DataFrame({"patient_id": [300, 200], "name": ["Remy", "Eve again"]})
#     db.load_pandas(df, labels="X", merge_primary_key="patient_id", merge_overwrite=False)
#     X_nodes = db.get_nodes(match_X)
#     expected = [{'patient_id': 100, 'name': 'Adam', 'age': 21},
#                 {'patient_id': 300, 'name': 'Remy'},
#                 {'patient_id': 200, 'name': 'Eve again', 'age': 19}]    # Notice that Eve's name got changed, but her age
#                                                                         #       was undisturbed b/c of merge_overwrite=False
#     assert compare_recordsets(X_nodes, expected)

#     df = pd.DataFrame({"patient_id": [300, 200], "name": ["Remy", "Eve YET again"]})
#     db.load_pandas(df, labels="X", merge_primary_key="patient_id", merge_overwrite=True)
#     X_nodes = db.get_nodes(match_X)
#     expected = [{'patient_id': 100, 'name': 'Adam', 'age': 21},
#                 {'patient_id': 300, 'name': 'Remy'},
#                 {'patient_id': 200, 'name': 'Eve YET again'}]    # Notice that Eve's name got changed, and her age got dropped
#     assert compare_recordsets(X_nodes, expected)

#     # Verify that another database Index got created as a result of again using merge_primary_key
#     all_indexes = db.get_indexes()
#     assert len(all_indexes) == 2

#     # There's no guarantee about the order of the Indices
#     index_0 = list(all_indexes.iloc[0])
#     index_1 = list(all_indexes.iloc[1])
#     expected_A = [['scientist'], 'scientist.scientist_id', ['scientist_id'], 'BTREE', 'NONUNIQUE']  # The old index
#     expected_B = [['X'], 'X.patient_id', ['patient_id'], 'BTREE', 'NONUNIQUE']                      # The newly-added index

#     assert (index_0 == expected_A) or (index_0 == expected_B)
#     if index_0 == expected_A:
#         assert index_1 == expected_B
#     else:
#         assert index_1 == expected_A


def test_load_pandas_3(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # First group on nodes to import
    ages = np.array([13, 25, 19, 99])
    series_1 = pd.Series(ages)    # A series with no name; during the import, "value" will be used
    id_list = db.load_pandas(series_1, labels="age")

    q = f'''
        MATCH (n :age) 
        WHERE id(n) IN {id_list}
        RETURN n
        '''
    res = db.query(q, single_column="n")
    expected = [{'value': 13}, {'value': 25}, {'value': 19}, {'value': 99}]
    assert compare_recordsets(res, expected)


    # More nodes to import
    prices = np.array([145, 512, 811])
    series_2 = pd.Series(prices, name="Discounted Price")   # This series has a bane
    id_list_2 = db.load_pandas(series_2, labels="store prices")

    # First, check that the old nodes are still there
    res = db.query(q, single_column="n")
    expected = [{'value': 13}, {'value': 25}, {'value': 19}, {'value': 99}]
    assert compare_recordsets(res, expected)
    # Now, look for the new nodes
    q = f'''
        MATCH (n :`store prices`) 
        WHERE id(n) IN {id_list_2}
        RETURN n
        '''
    res = db.query(q, single_column="n")
    expected = [{'Discounted Price': 145}, {'Discounted Price': 512}, {'Discounted Price': 811}]
    assert compare_recordsets(res, expected)


# def test_load_pandas_4(db):
#     # Test numeric columns
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     # First group on nodes to import, with option to ignore NaN's
#     df = pd.DataFrame({"name": ["pot", "pan", "microwave"], "price": [12, np.nan, 55]})
#     db.load_pandas(df, labels="inventory", ignore_nan=True)

#     imported_records = db.get_nodes(db.match(labels="inventory"))
#     expected = [{'price': 12.0, 'name': 'pot'}, {'name': 'pan'}, {'price': 55.0, 'name': 'microwave'}]
#     assert compare_recordsets(imported_records, expected)

#     # Re-import the same dataframe (with a different label), but this time not ignoring NaN's
#     db.load_pandas(df, labels="test", ignore_nan=False)

#     imported_records = db.get_nodes(db.match(labels="test"), order_by="name")
#     expected = [{'price': 55.0, 'name': 'microwave'}, {'price': np.nan, 'name': 'pan'}, {'price': 12.0, 'name': 'pot'}]
#     # Check the records not involving NaN
#     assert imported_records[0] == expected[0]
#     assert imported_records[2] == expected[2]
#     # Check the NaN record
#     assert imported_records[1]["name"] == expected[1]["name"]
#     assert np.isnan(imported_records[1]["price"])


# def test_load_pandas_4b(db):
#     # More tests of numeric columns
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     # Test with nans and ignore_nan = True
#     df = pd.DataFrame({"name": ["Bob", "Tom"], "col1": [26, None], "col2": [1.1, None]})
#     db.load_pandas(df, labels="X")
#     X_label_match = db.match(labels="X")
#     X_nodes = db.get_nodes(X_label_match)
#     expected = [{'name': 'Bob', 'col1': 26, 'col2': 1.1},
#                 {'name': 'Tom'}]
#     assert compare_recordsets(X_nodes, expected)


#     # Test of record merge with nans and ignore_nan = False
#     df = pd.DataFrame({"name": ["Bob", "Tom"], "col1": [26, None], "col2": [1.1, None]})
#     db.load_pandas(df, labels="X", merge_primary_key='name', merge_overwrite=False, ignore_nan=False)
#     X_nodes = db.get_nodes(X_label_match, order_by="name")
#     expected = [{'name': 'Bob', 'col1': 26, 'col2': 1.1},
#                 {'name': 'Tom', 'col1': np.nan, 'col2': np.nan}]

#     np.testing.assert_equal(X_nodes, expected)  # Two NaN's are treated as "equal" by this function


def test_load_pandas_4c(db):
    # Attempt to merge using columns with NULL values
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # Attempt to merge using columns with NULL values
    df = pd.DataFrame({"name": ["Bob", "Tom"], "col1": [26, None], "col2": [1.1, None]})
    with pytest.raises(Exception):
        # Cannot merge node on NULL value in column `col1`
        db.load_pandas(df, labels="X", merge_primary_key='col1', merge_overwrite=False)

    with pytest.raises(Exception):
        # Cannot merge node on NULL value in column `col1`
        db.load_pandas(df, labels="X", merge_primary_key='col1', merge_overwrite=True)


def test_load_pandas_5(db):
    db.empty_dbase(drop_indexes=False, drop_constraints=False)

    # First group on 5 nodes to import
    df = pd.DataFrame({"name": ["A", "B", "C", "D", "E"], "price": [1, 2, 3, 4, 5]})
    db.load_pandas(df, labels="inventory")

    imported_records = db.get_nodes(db.match(labels="inventory"))
    expected = [{'price': 1, 'name': 'A'}, {'price': 2, 'name': 'B'}, {'price': 3, 'name': 'C'}, {'price': 4, 'name': 'D'}, {'price': 5, 'name': 'E'}]
    assert compare_recordsets(imported_records, expected)


    # Re-import them (with a different label) in tiny "import chunks" of size 2
    db.load_pandas(df, labels="test", max_chunk_size=2)

    imported_records = db.get_nodes(db.match(labels="test"))
    expected = [{'price': 1, 'name': 'A'}, {'price': 2, 'name': 'B'}, {'price': 3, 'name': 'C'}, {'price': 4, 'name': 'D'}, {'price': 5, 'name': 'E'}]
    assert compare_recordsets(imported_records, expected)
    # Verify that the first import is also still there
    imported_records = db.get_nodes(db.match(labels="inventory"))
    assert compare_recordsets(imported_records, expected)

    # Verify that the data (all the 10 records) got imported as integers
    q = "MATCH (n) WHERE toInteger(n.price) = n.price RETURN count(n) AS number_integers"
    res = db.query(q, single_cell="number_integers")
    assert res == 10


# def test_load_pandas_6(db):
#     # Test times/dates
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     # Dataframe with group of dates, turned into a datetime column
#     df = pd.DataFrame({"name": ["A", "B", "C"], "arrival date": ["2020-01-01", "2020-01-11", "2020-01-21"]})
#     df['arrival date'] = pd.to_datetime(df['arrival date'])
#     id_list = db.load_pandas(df, labels="events")

#     for node_id in id_list:
#         q = f'''MATCH (n :events) WHERE id(n) = {node_id} RETURN apoc.meta.type(n.`arrival date`) AS dtype'''
#         res = db.query(q, single_cell="dtype")
#         assert res == "LocalDateTime"


# def test_load_pandas_7(db):
#     # More tests of times/dates
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     df = pd.DataFrame([[datetime(2019, 6, 1, 18, 40, 32, 0), date(2019, 6, 1)]], columns=["dtm", "dt"])
#     db.load_pandas(df, labels="MYTEST")
#     result = db.query("MATCH (x:MYTEST) return x.dtm as dtm, x.dt as dt", single_row=True)
#     print(result)
#     assert result == {'dtm': neo4j.time.DateTime(2019, 6, 1, 18, 40, 32, 0), 'dt': neo4j.time.Date(2019, 6, 1)}


# def test_load_pandas_8(db):
#     # More tests of times/dates
#     db.empty_dbase(drop_indexes=False, drop_constraints=False)

#     input_df = pd.DataFrame({
#         'int_values': [2, 1, 3, 4],
#         'str_values': ['abc', 'def', 'ghi', 'zzz'],
#         'start': [datetime(year=2010, month=1, day=1, hour=0, minute=1, second=2, microsecond=123),
#                   datetime(year=2023, month=1, day=1),
#                   pd.NaT,
#                   None]
#     })  # Note: for Pandas' datetime64[ns] types, NaT represents missing values

#     db.load_pandas(input_df, "MYTEST")
#     res = db.query("MATCH (x:MYTEST) RETURN x.start as start ORDER BY start")

#     assert res == [{'start': neo4j.time.DateTime(2010, 1, 1, 0, 1, 2, 123000)},
#                    {'start': neo4j.time.DateTime(2023, 1, 1, 0, 0, 0, 0)},
#                    {'start': None}, {'start': None}]



def test_pd_datetime_to_neo4j_datetime(db):
    # Prepare a dataframe with group of dates, turned into a datetime column
    df = pd.DataFrame({"name": ["A", "B", "C"], "my_date": ["2023-01-01", np.nan, "2023-01-21"]})
    df['my_date'] = pd.to_datetime(df['my_date'])

    # First, check the original dataframe
    date_col = list(df.my_date) # [Timestamp('2023-01-01 00:00:00'), Timestamp('2023-01-11 00:00:00'), ...]
    first_dt = date_col[0]      # Timestamp('2023-01-01 00:00:00')
    assert first_dt == pd.Timestamp('2023-01-01 00:00:00')


    result = db.pd_datetime_to_neo4j_datetime(df)

    result_col = list(result.my_date)   # [neo4j.time.DateTime(2023, 1, 1, 0, 0, 0, 0), ... ]

    assert result_col[0] == neo4j.time.DateTime(2023, 1, 1, 0, 0, 0, 0)
    assert result_col[1] == None
    assert result_col[2] == neo4j.time.DateTime(2023, 1, 21, 0, 0, 0, 0)

    assert id(df) != id(result)     # A clone of the original dataframe was created


    # If a dataframe is using strings rather than datetime value, no change will be made to it
    df = pd.DataFrame({"name": ["A", "B"], "my_date": ["2023-01-01", "2023-01-21"]})

    result = db.pd_datetime_to_neo4j_datetime(df)
    result_col = list(result.my_date)
    assert result_col == ['2023-01-01', '2023-01-21']

    assert id(df) == id(result)     # No cloning took place




###  ~ JSON IMPORT/EXPORT ~

# =>  SEE test_neoaccess_import_export.py




###  ~ DEBUGGING SUPPORT ~

def test_debug_print(db):
    pass    # TODO



def test_debug_trim(db):
    assert db.debug_trim("hello") == "hello"
    assert db.debug_trim(44) == "44"
    assert db.debug_trim("1234567890", max_len=5) == "12345 ..."


def test_debug_trim_print(db):
    pass    # TODO



def test_indent_chooser(db):
    pass    # TODO
