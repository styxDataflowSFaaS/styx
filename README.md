### STYX: A Transactional Deterministic SFAAS System running on Dataflows

#### Instructions

To run Styx quickly without needing a Kubernetes cluster, follow the following instructions. 

> NOTE: Styx requires Kafka as a replayable fault-tolerant ingress/egress and Minio to store the snapshots.


##### Kafka

To run kafka: `docker-compose -f docker-compose-kafka.yml up`

To clear kafka: `docker-compose -f docker-compose-kafka.yml down --volumes`

##### Minio

To run minio: `docker-compose -f docker-compose-minio.yml up`

To clear minio: `docker-compose -f docker-compose-minio.yml down --volumes`

##### Styx

To run Styx: `docker-compose up --build --scale worker=2`. Specify the number of workers required in the scale parameter.

To clear Styx: `docker-compose down --volumes`


#### Demo Instructions

Now that all the Styx required components are up for a simple demo, go to the `demo` folder. First, run the `kafka_output_comsumer.py` (rerun it if it throws a warning on topic creation), then run the `pure_kafka_demo.py`
To get performance metrics, kill the `kafka_output_comsumer.py` and then run `calculate_metrics.py`. This will Run the shopping cart demo within Styx.


For the NSDI 2024 submission, enter the `benchmark` folder and run the  `kafka_output_comsumer.py` as in the previous example. 
To run the workload, run the `client.py` with varying amounts in the `messages_per_second`, `sleeps_per_second`,
`sleep_time` and `seconds` parameters to generate a stable input throughput (a dry run to find the correct values is required)
and change the `zipf_const` to emulate the different scenarios.

##### Folder structure


*   [`benchmark`](https://github.com/styxDataflowSFaaS/styx/tree/main/benchmark) 
    The benchmark we used for the experiments for the NSDI 2024 submission regarding the bank transfer workload
    
*   [`coordinator`](https://github.com/styxDataflowSFaaS/styx/tree/main/coordinator) 
    Styx coordinator containerized python service code 
    
*   [`demo`](https://github.com/styxDataflowSFaaS/styx/tree/main/demo)
    Demo code with the "typical shopping cart application" the Styx application logic code can be found within the functions folder 

*   [`demo-deathstar-hotel-reservation`](https://github.com/styxDataflowSFaaS/styx/tree/main/demo)
    The benchmark we used for the experiments for the NSDI 2024 submission regarding the hotel reservation Deathstar workload   

*   [`demo-deathstar-movie-review`](https://github.com/styxDataflowSFaaS/styx/tree/main/demo)
    The benchmark we used for the experiments for the NSDI 2024 submission regarding the movie review Deathstar workload   

*   [`demo-tpc-c`](https://github.com/styxDataflowSFaaS/styx/tree/main/demo)
    The benchmark we used for the experiments for the NSDI 2024 submission regarding the TPC-C workload. For data generation, we used this open source TPC-C generator https://github.com/alexandervanrenen/tpcc-generator  

*   [`demo-ycsb`](https://github.com/styxDataflowSFaaS/styx/tree/main/demo-ycsb)
    Demo code with an implementation of the YCSB benchmark

*   [`env-example`](https://github.com/styxDataflowSFaaS/styx/tree/main/env-example)
    env folder example for the docker-compose Minio container

*   [`helm-config`](https://github.com/styxDataflowSFaaS/styx/tree/main/helm-config) 
    Configurations for the helm charts needed for the Styx deployment 

*   [`ingress`](https://github.com/styxDataflowSFaaS/styx/tree/main/ingress)
    **Deprecated! (we now use Kafka)** TCP ingress to the Styx cluster

*   [`k8s`](https://github.com/styxDataflowSFaaS/styx/tree/main/k8s)
    The Kubernetes deployment files for the Styx cluster

*   [`k8s-ingress`](https://github.com/styxDataflowSFaaS/styx/tree/main/k8s-ingress)
    Changes to the Kubernetes ingress since Styx requires opened TCP ports

*   [`locust-loadtest`](https://github.com/styxDataflowSFaaS/styx/tree/main/locust-loadtest)
    Locust load test for the shopping cart demo application

*   [`styx-package`](https://github.com/styxDataflowSFaaS/styx/tree/main/styx-package)
    The Styx framework Python package

*   [`tests`](https://github.com/styxDataflowSFaaS/styx/tree/main/tests)
    Tests for the worker components of Styx

*   [`worker`](https://github.com/styxDataflowSFaaS/styx/tree/main/styx-package)
    Styx worker containerized python service code 
