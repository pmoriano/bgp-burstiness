# bgp-burstiness

Contact: Pablo Moriano (pmoriano@indiana.edu)

This repository contains code for: 
- P. Moriano, R. Hill, and L. J. Camp, Using Bursty Announcements for Detecting BGP
Routing Anomalies, arXiv:1905.05835, 2019.

BibTeX entry:

```
@article{Moriano:2019:BGP:Burstiness,
    author = {P. Moriano, R. Hill, and L. J. Camp},
    title = {Using Bursty Announcements for Detecting BGP Routing Anomalies},
    journal = {arXiv preprint arXiv:1905.05835},
    year = {2019},
}
```

## Description

Run the following scripts and notebooks in the specified order to reproduce the each of the experiments in the paper. The script `helper_functions.py` is just a file that contains some common functions used across the experiments.

### Scripts

You will need to run the following scripts in `code` in the following order:

1. `compute-collector-evolution.py` --> "dic_feeders_evolution_" + incident + ".p"
2. `understanding-updates.py` --> "updates_dic_unique_" + incident +  ".json"
3. `understanding-interevent-time.py` --> "interevent_time_dic_" + incident + "_" + update_type + ".json"
4. `understanding-interevent-time-null.py` --> incident + '_' + 'null_interarrival_times_database.db'
5. `construct-routing-history-incidents-db.py` --> incident + '_' + 'routing_history_database.db'

## Notebooks

If you want to reproduce directly the figures in the paper download the processed datasets in:  

You will need to run the following notebooks in `notebooks` in the following order:

1. `01 - Understanding routes Indonesia.ipynb` --> Sections: Collectors’ disruption perception and Anomaly detection 
2. `02 - Understanding interevent times less time Indonesia.ipynb` --> Section Inter-arrival time analysis
3. `03 - MonteCarlo test Indonesia.ipynb` --> Section Inter-arrival time analysis


## Dependencies
These scripts were tested with following packages versions

	python		2.7.10
	pandas		0.23.0
	numpy		1.13.1
	scipy		1.1.0
	networkx	2.2

