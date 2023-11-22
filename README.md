# prometheus-data-scrapping-scheduler 
- prometheus raw data scrapping scheduler

This repository is a module for prometheus data scrapping scheduler on k8s environments; the configuration consists of three files, each of which is summarized as follows:

**promehteus_scrapper.py**: Integration that before [prometheus_data_refining_module](https://github.com/chromatices/k8s-prometheus-data-refining-module) three modules [prometheus_scrapper.py, prometheus_preprocessing.py, pod_scparate.py]. Add function of removing hash from replication pod's name with [nostril](https://github.com/casics/nostril.git) nonsence detector. 

**scheduler.py**: . 

**deployment.yaml**: 
------------
### Requirements
```
tqdm == 4.64.0
numpy == 1.21.5
pandas == 1.5.1
prometheus-api-client == 0.5.2
schedule == 1.1.0
pymysql == 1.0.2
sqlalchemy == 1.4.29
nostril package from [nostril](https://github.com/casics/nostril.git)
```

-----------------

### How to use
```
python3 prometheus_scrapper.py --url http://127.0.0.1:9090 --start_time 10d --end_time now --chunk_size [1s, 1t, 1h, 1d, 1m, 1y] --storage path [save dir]

python3 prometheus_preprocessing.py --storage_path [save_dir] --chunk_size [1s, 1t, 1h, 1d, 1m, 1y] --method [sum,mean,median]

python3 pod_separate.py --filename [prometheus_preprocessing.py 's output] --target_dir [prometheus_preprocessing.py 's output dir] --save_dir [final output dir]
```
------------------
- This work was supported by Institute for Information & communications Technology Promotion(IITP) grant funded by the Korea government(MSIT) (No.2021-0-00281, Development of highly integrated operator resource deployment optimization technology to maximize performance efficiency of high-load complex machine learning workloads in a hybrid cloud environment)

<!-- >>>>>>> aa9fb28b66f0adeadda5fcc24ea04af177947340 -->