备份数据脚本

1. 建表， 从源库拉取建表语句，在目的库建表。
python src/create_table.py config/config.cfg

2. 导出数据，从源库拉取数据，放入指定目录
python src/dump_data.py config/config.cfg

3. 写入目的库
python src/insert.py config/config.cfg
