df_p2 = spark.read.format("parquet").load("/FileStore/tables/part_00000_tid_4320459746949313749_5c3d407c_c844_4016_97ad_2edec446aa62_6688_1_c000_snappy.parquet")
df_p2.show(2)