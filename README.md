### Phases
#### 1. Phase-1
 - Cleanup phase
 - Remove table / tmp-table(s) from local Hive metastore and HDFS
 - All tables
 - Cmd(s)
   - `hive_drop_table_cmd`
   - `hadoop_rm_cmd`
 - Table(s)
   - **Case: Non-ORC**
     - 1: table
   - **Case: ORC**
     - *Case: Not-Joined*
       - 2: `table`, `tmp_table_1`
     - *Case: Joined*
       - 3: `table`, `tmp_table_1`, `tmp_table_2`

#### 2. Phase-2
 - MySQL phase
 - Copy table data from MySQL to HDFS (Hive-table)
 - All tables
 - Cmd
   - `sqoop_cmd`
 - Table
   - **Case: Non-ORC**
      - 1: `table`
   - **Case: ORC**
      - 1: `tmp_table_1`

#### 3. Phase-3
 - Hive ORC phase
 - Copy table data from Hive tmp_table_1 into either of following
   in order to convert text data into ORC format. Also perform partitioning if necessary
   - **Case: Not-Joined**
     - `table`
   - **Case: Joined**
     - `tmp_table_2`
 - Only ORC tables
 - Cmd(s)
   - **Table-creation**
     - *Case: Not-Joined*
       - `hive_create_table_cmd`
     - *Case: Joined*
       - `hive_create_tmp_table_2_cmd`
   - **Data-copying**
     - `hive_insert_select_cmd`
 - Table(s)
   - **Case: Not-Joined**
     - `table`
   - **Case: Joined**
     - `tmp_table_2`

#### 4. Phase-4
 - Hive Join phase
 - Copy data from Hive tmp_table_2 into Hive (final) table while dt-partitioning on a column obtained by joining with an existing table
 - Only ORC, Joined tables (big ORC tables that don't have a date-time column)
 - Cmd(s)
   - `hive_create_table_cmd`
   - `hive_insert_select_join_cmd`
 - Table
   - `table`

#### 5. Phase-5
 - S3 Copy phase
 - Copy data from HDFS to S3
 - All tables
 - Cmd(s)
   - `s3_delete_cmd`
   - `hadoop_distcp_cmd`
 - Table
   - `table`

#### 6. Phase-6
 - MSCK-Repair phase
 - Perform Hive MSCK REPAIR [on prod Hive metastore]
 - Partitioned tables
 - Cmd
   - `hive_msck_repair_cmd`
 - Table
   - `table`
