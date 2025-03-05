## Benchmarking of MobilityDB and MongoDB

- This project was created for my master's thesis and benchmarks MongoDB and MobilityDB in both single-node and distributed setups using spatiotemporal workloads. The cloud infrastructure is set up using terraform. Software systems are installed on the VMs using bash scripts. 
- For benchmarking deploy the respective Benchmarking Client application on another VM in the cloud. 
- The benchmarking manager interacts with the benchmarking clients to prepare the databases and run the benchmarks. Benchmark configuration can be adapted by modyfing the YAML file that is sent to the benchmarking clients. 
- Results are subsequently analyzed using jupyter notebook. 