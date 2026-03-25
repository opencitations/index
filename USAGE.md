<!--
SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>

SPDX-License-Identifier: ISC
-->

# OpenCitations: Index
## Usage
The proposed case use crossref as an example.

0. **Download** the dump
1. Execute the glob over the dump

    ```
    oc.index.glob.crossref -i ./input_dir -o ./output_dir
    ```
    1.1 **Only for crossref:  Run the **trim**

    ```
    oc.index.trim_crossref -i ./input_dir -o ./output_dir -m METADATA_FIELD -v METADATA_VALUE
    ```

    1.2 **Only if migration from csv is needed:  Run datasource utility in order to import data from csv to redis

    ```
    oc.index.datasource -o csv2redis -s INDEX_SERVICE -i ./glob_dir -id doi
    ```

2. Remove **existing** and **duplicated** entities

    2.1 Creates the new hash table for the index version on which an update is to be made 
    ```
    oc.index.moph -i ./input_dir -o ./moph_output
    ```
    where the input dir contain the index dataset downloaded from figshare.
    
    2.2 Run validate procedure to create the filtered version of the input
    ```
    oc.index.validate -s COCI -i ./dump_input -w n_workers -oci ./path_to_index_dataset -m ./moph_output -o ./dump_filtered
    ```
3. Create the new citations
    ```
    oc.index.cnc -i ./dump_input -o ./cnc_output -s COCI -w n_workers
    ```
