# ecscall
Simple wrapper for spreading a series of calls to a Python function across an
AWS ECS cluster. The cluster can use either Fargate (the simplest) or
user-configured EC2 instances to run the worker processes.

Example usage:

```python
import ecscall

def myFunc(a, b):
    "Do some big calculation"
    val = a + b
    return val

argTupleList = [
    (1, 2),
    (3, 4),
    (5, 6),
    (7, 8),
    (9, 10)
]
ecsClusterParams = ecscall.makeEcsClusterParams_Fargate(jobName='MyJob',
    # .... The rest of the AWS config arguments
    )
numWorkers = 3
retDict = ecscall.callFunc(myFunc, argTupleList, numWorkers,
    ecsClusterParams)
for ndx in sorted(retDict.keys()):
    args = argTupleList[ndx]
    value = retDict[ndx]
    print(args, value)
```

This will run the function myFunc on all the pairs given in argTupleList,
across 3 workers, and return a dictionary with a key for each index value,
and the corresponding function return value.

Full human-readable module documentation is in `ecscall.md`. The docstrings
for the helper functions `makeEcsClusterParams_Fargate` and 
`makeEcsClusterParams_PrivateCluster` describe in detail the AWS configuration
information required.

## Requirements
Requires `boto3` and `cloudpickle`.

The container being used to run the workers must have `cloudpickle` installed,
and also any other packages which the user function requires.
