# Parallel Yannakakis for DuckDB

## Overview
This project attempts to implement a **parallel version of the Yannakakis algorithm** within DuckDB with parallel hash tables and multi-threading to improve join performance. Ideally, this would achieve near-linear speedup as more CPU cores are utilized.

## Features
✅ **Single-threaded Yannakakis implementation** – A baseline for comparison.  
✅ **Parallel Yannakakis implementation** – Uses parallel hash joins for scalability.  
✅ **Performance benchmarking** – Measures speedup against DuckDB’s default join strategies.  

## Installation (TODO)

### **1. Clone the Repository**
```sh
git clone https://github.com/YOUR_USERNAME/ParallelYannakakis.git
cd ParallelYannakakis