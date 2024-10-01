# Abstract for Assignment 4: Key-Value Store

This is a remote in-memory key-value store developed as part of the Operating Systems course at Vrije Universiteit Amsterdam (VU). The project involved implementing a multithreaded server capable of handling multiple client requests concurrently. 

Key components included:

- **Data Structure**: A hashtable was employed to store key-value pairs, with operations for insertion, retrieval, and deletion.

- **Concurrency Control**: The implementation focused on thread safety through the use of locks and synchronization mechanisms, enabling simultaneous access to different keys while preventing conflicts.

- **Command Protocol**: A text-based protocol was defined to facilitate communication between clients and the server, allowing commands like SET, GET, and DEL.

This assignment provided practical experience in designing and implementing a concurrent system, reinforcing fundamental concepts in operating systems and multithreading.
