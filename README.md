# AwesomeDML

Awesome DML is a Simple Distributed Machine Learning system which handles large data sets and provides a CLI interface to RUN inference jobs on pretrained ResNet50 and InceptionV3 jobs.

SDFS uses scp internally to download files from a host. The username and password for host should be provided using `password.txt` file with `username,password` format.

## Usage

### STEP-1

Edit the `config.py` file in `introduce process` and `awesomedml` with the information about all the available nodes and the GLOBAL_RING_TOPOLOGY for all the nodes.

Enter the `hostname` or `IP` and `Port` for the node tobe monitor.

```python

H1: final = Node('127.0.0.1', 8001, 'H1')
H2: final = Node('127.0.0.1', 8002, 'H2')
H3: final = Node('127.0.0.1', 8003, 'H3')
H4: final = Node('127.0.0.1', 8004, 'H4')
H5: final = Node('127.0.0.1', 8005, 'H5')
H6: final = Node('127.0.0.1', 8006, 'H6')
H7: final = Node('127.0.0.1', 8007, 'H7')
H8: final = Node('127.0.0.1', 8008, 'H8')
H9: final = Node('127.0.0.1', 8009, 'H9')
H10: final = Node('127.0.0.1', 8010, 'H10')

GLOBAL_RING_TOPOLOGY: dict = {

    H1: [H2, H10, H5],

    H2: [H3, H1, H6],

    H3: [H4, H2, H7],

    H4: [H5, H3, H8],

    H5: [H6, H4, H9],

    H6: [H7, H5, H10],

    H7: [H8, H6, H1],

    H8: [H9, H7, H2],

    H9: [H10, H8, H3],

    H10: [H1, H9, H4]

}
```

The above example configure the failure detector to monitor 10 process running on 10 different ports locally.

To configure the Completness and Accuracy of the failure detector follow below steps.

Edit the following fields in `config.py`:

1. `M`: Parameter to configure the completness of the system. configues the system to handle max number of simultanious failures.

2. `PING_TIMEOOUT`: The time to wait for the ACK from the node.

3. `PING_DURATION`: Parameter to configure frequency of PINGs.

4. `CLEANUP_TIME`: The time out to mark a suspected node as failure and remove from membership list.

The below configuration allows the failure detector to handle upto 3 failures and still maintain the 100% completeness. The M value can be changes to any number depending on the requirement. The PING_TIMEOUT can be adjusted such a way to improve the accuracy of failure detection and it depends on the network latency. The PING_DURATION lets users control the number of bytes flowing in the network, this parameter depends on the network capability. The CLEANUP_TIME gives a suspected node some additional time to make available online, this parameter depends on the network congestion and intermitent network issues in the system.

```python

M: final = 3

PING_TIMEOOUT: final = 2

PING_DURATION: final = 2.5

CLEANUP_TIME: final = 10

```

### STEP-2

Start the introducer process.

```console
$ cd ~/awesomedml/introduce\ process/
$ python3 main.py'
```

The introduces will run on port 8888.

### STEP-3

Once the nodes and global ring topology is updated. RUN the below command.

The `main.py` has the starter code to initialize and star the failure detector.

To run the application following options has to be passed.

`--hostname=`   : The current hostname or IP of the node.
`--port=`       : The current Port of the node.
`-t`            : Which allows application to run in test mode.

> **_NOTE:_** Introducer node should be up and running before starting other nodes.

```console
$ cd ~/awesomedml/
$ python3.9 main.py --hostname="127.0.0.1" --port=8000
```

### STEP-4

Once the application is stated the `awesomedml` provides a console to interact with the running application with following options and commands.

Option C1: Prints the Query rates and Query count per model

Option C2: Prints the average, standard deviation and percentiles of Query processing times per model

Option C3: Sets the batch size per model

Option C4: Submits job for each model and also get inference outputs

Option C5: Deplay the current assigned jobs on each VM

```console
choose one of the following options or type commands:

MP4 commands:

 C1: Query Rate (10sec) & Query Count [Per model]

 C2: Query Processing Time: [Average, Percentiles, Standard Deviation]

 C3 <InceptionV3|ResNet50> <Batch Size>

 C4: submit-job <InceptionV3|ResNet50> <num:of images>

 C4: get-output <jobid>

 C5: Display current assigned jobs



options:

 1. list the membership list.

 2. list self id.

 3. join the group.

 4. leave the group.

 5. load testfiles into sdfs.

 6. print files stored per node.

 7. print all files in the SDFS.

 8. print number of files in the SDFS.



commands:

 * put <localfilename> <sdfsfilename>

 * get <sdfsfilename> <localfilename>

 * get-all <sdfsfilepattern> <local_dir>

 * delete <sdfsfilename>

 * ls <sdfsfilename>

 * ls-all <sdfsfilepattern>

 * store

 * get-versions <sdfsfilename> <numversions> <localfilename>
```
