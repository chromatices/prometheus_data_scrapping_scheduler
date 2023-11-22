# prometheus-data-scrapping-scheduler 
- prometheus raw data scrapping scheduler

This repository is a module for prometheus data scrapping scheduler on k8s environments; the configuration consists of three files, each of which is summarized as follows:

**promehteus_scrapper.py**: Integration that before [prometheus_data_refining_module](https://github.com/chromatices/k8s-prometheus-data-refining-module) three modules [prometheus_scrapper.py, prometheus_preprocessing.py, pod_scparate.py]. Add function of removing hash from replication pod's name with [nostril](https://github.com/casics/nostril.git) nonsence detector. 

**scheduler.py**: Core scheduler of prometheus metric data scrapping module. You can set scheduler unit, scrapping unit, preprocessing unit.     

**deployment.yaml**: Sample of scheduler on k8s.     


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

------------------
- This work was supported by Institute for Information & communications Technology Promotion(IITP) grant funded by the Korea government(MSIT) (No.2021-0-00281, Development of highly integrated operator resource deployment optimization technology to maximize performance efficiency of high-load complex machine learning workloads in a hybrid cloud environment)

<!-- >>>>>>> aa9fb28b66f0adeadda5fcc24ea04af177947340 -->