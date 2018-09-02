# Amazon Dynamo

## Aim

To build a distributed key-value storage system based on Amazon Dynamo which provides availability and linearizability using chain replication. It also provide successful reads even in the presence of failure

## Testing

The grading script is used to test the behaviour of the application. This is a phase by phase grading script and the following are phase descriptions,

1. Testing basic operations: This phase will test insert, query, and delete (including the special keys @ and *). This will ensure that all data is correctly replicated. It will not perform any concurrent operations or test failures.
2. Testing concurrent operations with differing keys: This phase will test your implementation under concurrent operations, but without failure. The tester will use different key-value pairs inserted and queried concurrently on various nodes.
3. Testing concurrent operations with like keys: This phase will test your implementation under concurrent operations with the same keys, but no failure. The tester will use the same set of key-value pairs inserted and queried concurrently on all nodes.
4. Testing one failure: This phase will test every operation under a single failure.For each operation, one node will crash before the operation starts. After the operation is done, the node will recover. This will be repeated for each operation.
5. Testing concurrent operations with one failure: This phase will execute various operations concurrently and crash one node in the middle of execution. After some time, the node will recover (also during execution).
6. Testing concurrent operations with repeated failures: This phase will crash one node at a time, repeatedly. That is, one node will crash and then recover during execution, and then after its recovery another node will crash and then recover, etc. There will be a brief period of time between each crash-recover sequence.

How to run the grading script: (Use python-2.7)

The python files and the grading scripts can be found under the scripts directory.

1. Create AVD: python create_avd.py
2. Start AVD (5 AVDs): python run_avd.py 5
3. Start the emulator network using port 10000: python set_redir.py 10000
4. Test the APK by running the grading script and pass the APK file to the program. [grading_script] [apk_name].apk

#### For more information about grading and detailed instruction please refer the simple-dynamo.pdf
