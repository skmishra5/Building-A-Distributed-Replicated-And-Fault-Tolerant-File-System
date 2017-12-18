The important subsystems associated with the project is given below.

  1. Chunk Server responsible for managing file chunks. There will be one instance of the chunk
     server running on each machine.
  2. A controller node for managing information about chunk servers and chunks within the system.
     There will be only 1 instance of the controller node.
  3. Client which is responsible for storing, retrieving, and updating files in the system.

**Fragment and Distribute**

In this file system, portions (or chunks) of a file are dispersed on a set of available machines. There
are multiple chunk servers in the system: on each machine there can be at most one chunk server that
is responsible for managing chunks belonging to different files. A chunk server stores these chunks on
its local disk.

Every file that will be stored in this file system will be split into 64KB chunks. These chunks need to be
distributed on a set of available chunk servers. Each 64KB chunk keeps track of its own integrity, by
maintaining checksums for 8KB slices of the chunk. The message digest algorithm to be used for
computing this checksum is SHA-1: this returns a 160-bit digest for a set of bytes. Individual chunks
will be stored as regular files on the host file system.

File writes/reads will be done via the chunk servers that hold portions of the file. The chunk server adds
integrity information to individual chunks before writing them to disk. Reads done by the chunk server
will check for integrity of the chunk slices and will send only the content to the client (the integrity
information is not sent).

Each chunk being stored to a file needs to have metadata associated with it. If the file name is
/user/bob/experiment/SimFile.data, chunk 2 of this file will be stored by a chunk server as
/user/bob/experiment/SimFile.data_chunk2. This is an example of the metadata being encoded in
the name of the file. There will be other metadata associated with the chunk: this additional information
should not be encoded in the filename; this includes –
  • Versioning Information: Multiple writes to the chunk will increment the version number
    associated with the chunk.
  • Sequencing Information: There will be a sequence number associated with each chunk.
  • File name: The file that the chunk is a part of
  • Timestamp: The time that it was last updated. 

**Chunk Server and the Controller Node**

Each chunk server will maintain a list of the files that it manages. For each file, the chunk server will
maintain information about the chunks that it holds.

There will be one controller node in the system. This node is responsible for tracking information about
the chunks held by various chunk servers in the system. It achieves this via heartbeats that are
periodically exchanged between the controller and chunk servers. The controller is also responsible for
tracking live chunk servers in the system. The controller does not store anything on disk, all information
about the chunk servers and the chunks that they hold are maintained in memory.

**Heartbeats**

The Controller Node will run on a preset host/port. A chunk server will regularly send heartbeats to the
controller node. These heartbeats will be split into two
  1. A major heartbeat every 5 minutes
  2. A Minor heartbeat every 30 seconds
At the 5 minute mark ONLY the major heartbeat should be sent out.

The major heartbeat will include metadata information about ALL the chunks maintained at the chunk
server. The minor heartbeat will include information about any newly added chunks. Additionally, when
a chunk server detects file corruption, it will report this to the Controller Node.

All heartbeats will include information about the total number of chunks and free-space available at the
chunk server. Free space information should be one of the metrics used for distribution of chunks on
the set of available commodity machines.

The Controller will also send heartbeats to the chunk servers to detect failures.

**Replication of files**

Each file should have a replication level of 3; this means that every chunk within the file should be
replicated at least 3 times. When a client contacts the Controller node to write a file, the Controller will
return a list of 3 chunk servers to which a chunk (64KB) can be written. The client then contacts these
chunk servers to store the file. Rather than write to each chunk server directly, if there are 3 chunk
servers A, B and C that were returned by the controller, the client will only write to chunk server A,
which is responsible for forwarding the chunk to B, which in turn is responsible for forwarding it C.
Propagation chunks in this fashion has the advantage of utilizing the bandwidths more efficiently. After
the first 64KB chunk of a file has been written, the client (this should be managed transparently by your
API) contacts the Controller to write the next chunk and repeat the process. A given chunk server cannot
hold more than one replica of a given chunk.

Chunk data will be sent to the chunk servers and not the controller. The controller is only responsible
for pointing the client to the chunk servers: chunk data should not flow through the controller.

**Disperse a file on a set of available chunks servers**

You will take a file and ensure the storage of chunks of this file on different chunk servers. Each chunk
of the file should be replicated 3 times. This chunk should be available on the local disk (/tmp) of the
chunk server.

**Reading a previously stored file**

During the testing process, you will have to read the file that was previously scattered over a set of
chunk servers. For reading each 64 KB chunk, the client will contact the Controller and retrieve
information about the chunk server that holds the chunk. Assuming there were no failures, the file read
should match the file that was dispersed.

**Tampering with chunks**

Next, we will go to an individual chunk file managed by your File System and tamper this by modifying
the content of the file. This may be deleting/adding a line or a word to the file: this is done outside the
purview of your chunk server. This should cause the file read to report a data corruption, and the specific
chunk (and slice within it) that was corrupted.

**Error Correction**

The contents of one of your chunks will be tampered with. A subsequent read of the file should detect
this corruption and initiate a fix of this chunk slice.

If it is detected that a slice of a chunk is corrupted, contact other valid replicas of this chunk and perform
error correction for the chunk slice. Error detections will be performed outside the heartbeat control
message scheme. The control flow is through the Controller, but the data flow is between the chunk
servers.

**Coping with failures of chunk servers**

We will terminate one/more of the chunk servers. In response to detection of failures of the chunk
servers, the Controller should contact chunk servers that hold legitimate copies of the affected chunks
and have them send these chunks to designated chunk servers. Note: The control flow is through the
Controller, but the data flow is between the chunk servers.
The metadata maintained at the Controller is updated to reflect this. How are reads handled during this
failure?

**Coping with the failure of the Controller**

The Controller process is terminated, and after some time it is restarted. The Controller receives
minor/major heartbeats from the chunk servers. In response to receiving a minor update from a chunk
server, the (recovering) Controller must contact the chunk server and request a major heartbeat. The
controller SHOULD NOT maintain any persistent information (i.e. it should not write to its stable storage)
about the locations of the chunk servers.
