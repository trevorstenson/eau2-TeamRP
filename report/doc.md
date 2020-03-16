# Eau2 High-level Design

### By TeamRP

### Introduction

The eau2 system that we have been tasked with implementing can be described as a distributed big-data analysis tool that allows for complex computations and operations on large amounts of data stored in a columnar format.

### Architecture

From a high-level, the major components required to fully implement the eau2 application are a general distributed key value store, a distributed Array and DataFrame that serve as an abstraction layer on top of the key value store, and the final application layer where any code making use of DataFrames is written. The distributed key value store is the most important building block in the application as it encapsulates all of the socket-based networking functionality required to manage and orchestrate a set of nodes storing part of the application’s data in independent key value stores. This layer can be interacted with just like a conventional key-value storage data structure because all of the networking logic will be hidden from the user internally.

The next level of the application contains implementations of both distributed Arrays and DataFrames. Each of these classes would provide the same functionality as our previously implemented standard Arrays and DataFrames. Internally, both classes would make use of the distributed key-value store in order to hold chunks of their data as serialized blobs until they were needed. This allows for massive amounts of data to be split up over a set of nodes on the network.

The final layer of code needed for the eau2 system is the application layer where any arbitrary code can be written to interface with the distributed DataFrames and Arrays.

### Implementation

The first thing required to implement a fully functioning eau2 system is a distributed key-value store that utilizes socket-based networking to communicate between nodes. A Key consists of a String used for searching for the corresponding value, as well as an id representing the ID of the node that the data is stored on. A Value is just a serialized blob of data that can easily be passed through socket connections until it needs to be deserialized into a corresponding object. On each Node of the distributed key-value store, a local key-value store will be implemented in the form of a standard C++ Map from String keys to serialized blob values. The DistributedStore object will manage startup of all Nodes used to store portions of data, handle communication to and from Nodes, and update Nodes of server changes much like our previous server implementation. Additionally it will include new functionality for methods like get, put, and getAndWait. The get method takes a Key object and searches the network of Nodes for the corresponding serialized data and returns it. The put method allows for inserting a Value blob under a new Key in the key-value store. The getAndWait method serves the same purpose as get, but blocks program operation until a value is returned from the distributed store. A distributed array would be implemented utilizing the distributed key-value store described above. 

### Uses Cases
The eau2 system will mainly be used for distributed computing on large datasets. Thus, one would provide the system with a schema on read file, which will be ingested and transformed into a distributed DataFrame. This is particularly useful when the size of the data is too large to be held on one device. The eau2 system allows the data to be broken up into manageable sizes. 

From here, a user can specify operations they wish to perform on the data. The operations that need to be supported are not fully specified yet, and this is one of our groups’ open questions. Therefore, the exact eau2 API is not fully fleshed out, but in general, it allows for operating on large datasets. We expect to have to support iterating through the DataFrame in a similar fashion to Rower and Fielder from previous assignments.

### Open Questions
One question we have is what kinds of operations should we be supporting when operating on the DataFrame? The speed of the operations supported relative to the speed of a network communication may change how we want to structure the distributed DataFrame. If the operations are very fast, we want to be sure to minimize network calls required to parse the entire DataFrame. However, if the operations are long and intensive, then many network calls will not have as great of a relative effect on execution speed.

A second question is how much we can expect data to change once it has been read in from a schema on read file. This will affect how we balance data between different nodes. If we can expect the size of the DataFrame to remain relatively constant, then it will likely be best to equally distribute all data across the nodes. However, if we expect the data set to change, this may not be the optimal route.

### Status
A prototype implementation has been created, assembling the various components of the eau2 system. While the majority of the pieces are present, the pieces are not all connected at the moment.
To start, the schema-on-read adapter has been hooked up to our original DataFrame. We can be sure that we are able to read in files, all that needs to be changed is the way a DataFrame stores data. The next step is to change the storage of values in columns to distributed storage across multiple nodes. 

Additionally, the network layer has been built, including the serialization of all messages and data types needed for communication among nodes. However, the actual logic to connect and pass data between nodes has not been implemented. The next step is to get nodes to recognize DataFrame data, as well as be able to provide stored data on request. 
To connect the DataFrame to the network layer, we also need to implement some of the classes sitting in between, such as DistributedStore and DistributedArray. 

	

