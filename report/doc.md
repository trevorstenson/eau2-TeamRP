# Eau2 High-level Design

### By TeamRP

### Introduction

The eau2 system that we have been tasked with implementing can be described as a distributed big-data analysis tool that allows for complex computations and operations on large amounts of data stored in a columnar format.

#

### Status
We have successfully implemented basic functionality for the Linus application by building upon the starter code provided. 
Currently, our application successfully runs on a single node without any networking functionality for a subset of data. 
We decided to first implement it this way because we were not fully confident in our networking layer's reliability due to both connectivity issues during network setup, as well as a lack of failure mechanisms when data happened to be lost. 
Because the Linus application requires so much data to fully process the result, we did not trust our initial networking layer to handle that load. 
Additionally, it was much easier to debug the Linus functionality without worrying about any potential networking issues popping up.
A summary of completed components is below. 

## Completed components
* SOR adapter is integrated with DataFrame
* Processing of DataFrames with Fielders, Rowers, and Visitors
* Serialization of DataFrames and Messages
* Exchange of DataFrames between KVStores on separate nodes, however inconsistently (Demo-able with M4 WordCount)
* Linus has been adapted to our implementation, however it will only successfully run on 1 node

## Outstanding issues
* Networking inconsistencies

#

### Architecture

From a high-level, the major components required to fully implement the eau2 application are a general distributed key value store, a distributed Array and DataFrame that serve as an abstraction layer on top of the key value store, and the final application layer where any code making use of DataFrames is written. The distributed key value store is the most important building block in the application as it encapsulates all of the socket-based networking functionality required to manage and orchestrate a set of nodes storing part of the application’s data in independent key value stores. This layer can be interacted with just like a conventional key-value storage data structure because all of the networking logic will be hidden from the user internally.

The next level of the application contains implementations of both distributed Arrays and DataFrames. Each of these classes would provide the same functionality as our previously implemented standard Arrays and DataFrames. Internally, both classes would make use of the distributed key-value store in order to hold chunks of their data as serialized blobs until they were needed. This allows for massive amounts of data to be split up over a set of nodes on the network.

The final layer of code needed for the eau2 system is the application layer where any arbitrary code can be written to interface with the distributed DataFrames and Arrays.

#

### Implementation

The first thing required to implement a fully functioning eau2 system is a distributed key-value store that utilizes socket-based networking to communicate between nodes. A Key consists of a String used for searching for the corresponding value, as well as an id representing the ID of the node that the data is stored on. A Value is just a serialized blob of data that can easily be passed through socket connections until it needs to be deserialized into a corresponding object. 

Each node on the network is essentially a local key-value store with the ability to ask other nodes on the network for data stored in their local key-value stores. Because a Key contains information about what node data is stored on, we are able to send a request to the corresponding node over the network. Currently our KVStore class maintains its own local C++ Map and a size_t representing the index associated with the current application node. The main functionality supported by our KVStore are the methods get, put, and waitAndGet. The get method takes a Key object and searches the network of Nodes for the corresponding serialized data and returns it. The put method allows for inserting a Value blob under a new Key in the key-value store. If the key’s index matches the local store index, the data is stored locally. If not, the data is sent to the correct node for local storage. The getAndWait method serves the same purpose as get, but blocks program operation until a value is returned from the distributed store. This method exists because it may take time for accessing necessary data that is stored on other nodes.

The networking functionality encapsulated within the KVStore class is not activated until a child class of Application calls its run_() method. This method calls the method that will register the KVStore with the server and neighboring nodes, as well as executing whatever application-level functionality it needs to support.

#

### Uses Cases
The eau2 system will mainly be used for distributed computing on large datasets. Thus, one would provide the system with a schema on read file, which will be ingested and transformed into a distributed DataFrame. This is particularly useful when the size of the data is too large to be held on one device. The eau2 system allows the data to be broken up into manageable sizes. 

From here, a user can specify operations they wish to perform on the data. The operations that need to be supported are not fully specified yet, and this is one of our groups’ open questions. Therefore, the exact eau2 API is not fully fleshed out, but in general, it allows for operating on large datasets. We expect to have to support iterating through the DataFrame in a similar fashion to Rower and Fielder from previous assignments.

Currently, we have provided application code for two realistic use-cases. The first application reads in a text file and counts the occurrences for each unique word. 
The second application accomplishes the much more difficult task of computing all github users who have worked on software projects in some capacity with Linus Torvalds up to seven degrees away.
