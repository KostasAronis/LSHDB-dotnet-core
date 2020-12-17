# LSHDB-dotnet-core
![GitHub](https://img.shields.io/github/license/KostasAronis/LSHDB-dotnet-core?style=plastic)

### Disclaimer
> This repository is a partial translation from java to c# with the appropriate best practice changes from the original [LSHDB repo](https://github.com/dimkar121/LSHDB).  
The following description of LSHDB is taken from the original repo as-is.

>LSHDB is a parallel and distributed data engine, which relies on the locality-sensitive hashing (LSH) technique and noSQL systems, for performing record linkage (including privacy-preserving record linkage - PPRL) and similarity search tasks. Parallelism lies at the core of its mechanism, since queries are executed in parallel using a pool of threads.
The relevant demo paper "LSHDB: A Parallel and Distributed Engine for Record Linkage and Similarity Search" by Dimitrios Karapiperis (HoU), Aris Gkoulalas-Divanis (IBM), and Vassilios S. Verykios (HoU) was presented in IEEE ICDM 2016, which was held in Barcelona, Spain.

The initial goal of this repo is to reproduce the standalone LSHDB storage, in essence an executable that accepts some command line arguments, one of which is the input file name and stores the entries of the file in an lshdb data storage.
