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

Follow the below procedure to reproduce the each of the experiments in the paper.

### Scripts

Yo will need to run the following scripts in `code` in the following order:
1. compute-collector-evolution.py --> "dic_feeders_evolution_" + incident + ".p"
2. understanding-updates.py
3. understanding-interevent-time.py
4. 



## Dependencies
These scripts were tested with following packages versions

	python		2.7.10
	pandas		0.23.0
	numpy		1.13.1
	scipy		1.1.0
	networkx	2.2

