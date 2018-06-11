# The Raft Algorithm: A Python Implementation

This is a version of the Raft consensus algorithm, as described in [Diego Ongaro's paper](https://raft.github.io/raft.pdf), implemented in Python. If you want to learn more about the algorithm and how it works, I have given some [conference talks](http://pyvideo.org/speaker/laura-hampton.html) about it.

In this implementation, I used Python 3.5 and David Beazley's [Curio](https://github.com/dabeaz/curio) library to manage the asynchronous functions. The code includes a node class and a client class.

There are still a few TODOs outstanding:

- Finish integration testing on log propagation
- Implement voting
- Write tests (I realized with this project that I should start writing tests earlier.)
