##!/usr/bin/python3.7.2
# -*- coding: utf-8 -*-
# @Time   : 2019-07-17
# @Author : wenhan.ji
# @Email : henryji96@outlook.com
# @Team : ETL
# @Desc : ==============================================
# Life is Short I Use Python!!!===
# HBase增删查工具类 ===
#  ===
#  ===
# ======================================================
# @Project : HBase_Util# @FileName: hbaseUtil.py
import happybase
import logging
logging.basicConfig(filename="hbaseUtil.log", level=logging.INFO)

class hbaseUtil():
    def __init__(self, host, size, table_prefix=None):
        try:
            self.hbase_pool = happybase.ConnectionPool(host=host, size=size, table_prefix=table_prefix)
            logging.info("连接hbase，host：{}，size：{}， table_prefix：{}".format(host, size, table_prefix))
        except BaseException:
            logging.info("连接hbase异常，host：{}，size：{}，table_prefix：{}".format(host, size, table_prefix))
            exit()

#
# 打印初始化时指定table_prefix为前缀的所有表
#
    def listTables(self):

        with self.hbase_pool.connection() as conn:
            print(conn.tables())

#
# 创建表
# @param tableName表名 families列族信息
# families格式:
# {
#     'cf1': dict(),
#     'cf2': dict(),
#     'cf3': {
#             'max_versions': 3,
#             'bloom_filter_vector_size': 0,
#             'name': 'cf: ',
#             'bloom_filter_type': 'NONE',
#             'bloom_filter_nb_hashes': 0,
#             'time_to_live': 2147483647,
#             'in_memory': False,
#             'block_cache_enabled': False,
#             'compression': 'NONE'}
# }
    def createTable(self, tableName, families):

        with self.hbase_pool.connection() as conn:
            try:
                conn.create_table(tableName,families)
                logging.info("创建表 {} 成功".format(tableName))
            except BaseException:
                logging.info("创建表 {} 异常".format(tableName))
                exit()
#
# 删除表
# @param tableName表名
#
    def deleteTable(self, tableName):

        with self.hbase_pool.connection() as conn:

            try:
                conn.delete_table(tableName, disable=True)
                logging.info("删除表 {} 成功".format(tableName))
            except BaseException:
                logging.info("删除表 {} 失败".format(tableName))
                exit()

#
# 禁用表
# @param tableName表名
#
    def disableTable(self, tableName):
        with self.hbase_pool.connection() as conn:
            conn.disable_table(tableName)
#
# 启用表
# @param tableName表名
#
    def enableTable(self, tableName):
        with self.hbase_pool.connection() as conn:
            conn.enable_table(tableName)
#
# 打印列族信息
# @param tableName表名
#
    def columnFamilyInfo(self, tableName):
        with self.hbase_pool.connection() as conn:
            table = conn.table(tableName)
            for k,v in table.families().items():
                print(k,v)

#
# 插入一个元素
# @param tableName表名 rowkey行 column列 putData插入数据
# column: 'columnFamily:column'
    def putOneCell(self, tableName, rowkey, column, putData):
        with self.hbase_pool.connection() as conn:
            table = conn.table(tableName)
            try:
                table.put(row=rowkey, data={column: putData})
                logging.info("{}表 插入单个数据成功".format(tableName))
            except BaseException:
                logging.info("{}表 插入单个数据异常".format(tableName))
                exit()

#
# 插入一行
# @param tableName表名 rowkey行 keyValDict
# keyValDict格式，key为列，value为插入数据:
#     {'columnFamily1:column1': value1,
#      'columnFamily1:column2': value2,
#      'columnFamily2:column1': value3}
    def putOneRow(self, tableName, rowkey, keyValDict):
        with self.hbase_pool.connection() as conn:
            table = conn.table(tableName)
            try:
                table.put(row=rowkey, data=keyValDict)
                logging.info("{}表 插入一行数据成功".format(tableName))
            except BaseException:
                logging.info("{}表 插入一行数据异常".format(tableName))
                exit()
#
# 插入多行
# @param tableName表名 keyValDict字典 batch_size批量写的大小
# keyValDict格式，key为行，value为指明列与插入数据的字典:
# {'rowKey1': {'columnFamily1:column1': value1, 'columnFamily1:column2': value2},
#  'rowKey2': {'columnFamily2:column1': value3, 'columnFamily1:column2': value4},
#  'rowKey3': {'columnFamily2:column1': value5, 'columnFamily1:column2': value6}}
    def putMultipleRows(self, tableName, keyValDict, batch_size=100):

        with self.hbase_pool.connection() as conn:
            table = conn.table(tableName)

            try:
                with table.batch(batch_size=batch_size) as bat:
                    for rowkey, dt in keyValDict.items():
                        bat.put(rowkey, dt)
                logging.info("{}表 批量写入数据成功".format(tableName))
            except BaseException:
                logging.info("{}表 批量写入数据异常".format(tableName))
                exit()

#
# 读一个元素
# @param tableName表名 rowKey行  timestamp指定时间戳 include_timestamp返回值包含时间戳
#        column: 'columnFamily1:column1'
#        versions: 为None返回所有版本， 为n返回最近n个版本
# @return 当只返回最近版本时，返回一个值； 当返回多个版本时，返回一个list
    def getOneCell(self, tableName, rowKey, column, versions=1, timestamp=None, include_timestamp=False):
        with self.hbase_pool.connection() as conn:
            table = conn.table(tableName)
            try:
                result = table.cells(row=rowKey, column=column, versions=versions, timestamp=timestamp, include_timestamp=include_timestamp)
                if versions == 1:
                    return result[0]
                else:
                    return result
                logging.info("{}表 读取数据成功".format(tableName))
            except BaseException:
                logging.info("{}表 读取数据异常".format(tableName))
                exit()

#
# 读一行数据
# @param tableName表名 rowKey行 timestamp指定时间戳 include_timestamp返回值包含时间戳
#        columns: 列表包含所有要读取的列 'columnFamily1:column1'
# @return 返回包含一行数据的字典
    def getOneRow(self, tableName, rowKey, columns=None, timestamp=None, include_timestamp=False):
        with self.hbase_pool.connection() as conn:
            table = conn.table(tableName)
            try:
                result = table.row(row=rowKey, columns=columns, timestamp=timestamp, include_timestamp=include_timestamp)
                return result
                logging.info("{}表 读取一行数据成功".format(tableName))
            except BaseException:
                logging.info("{}表 读取一行数据异常".format(tableName))
                exit()
#
# 读多行数据
# @param tableName表名  timestamp指定时间戳 include_timestamp返回值包含时间戳
#        rowKeys: 需要读取行的列表
#        columns: 包含所有要读取的列的列表 ['columnFamily1:column1', 'columnFamily1:column2']
# @return 返回一个list，每个元素为一个tuple。 tuple[0]为rowkey，tuple[1]为包含一行数据的字典
    def getMultipleRows(self, tableName, rowKeys, columns=None, timestamp=None, include_timestamp=False):
        with self.hbase_pool.connection() as conn:
            table = conn.table(tableName)
            try:
                result = table.rows(rows=rowKeys, columns=columns, timestamp=timestamp, include_timestamp=include_timestamp)
                return result
                logging.info("{}表 读取多行数据成功".format(tableName))
            except BaseException:
                logging.info("{}表 读取多行数据异常".format(tableName))
                exit()
#
# 读多行数据
# @param tableName表名 row_start开始迭代的行，row_stop停止迭代的行（不包含）
# Possible Filters(https://www.jianshu.com/p/0bad3534186b):
#   "KeyOnlyFilter()"                                               all value set to null
#   "FirstKeyOnlyFilter()                                           all rowkey's first cf:column value
#   "PrefixFilter('prefix')"                                        specify rowkey prefix
#   "ColumnPrefixFilter('prefix')"                                  specify column prefix(not column family)
#   "MultipleColumnPrefixFilter('p1', 'p2')"                        specify multiple column prefix(not column family)
#   "InclusiveStopFilter('rowkey')"                                 scan until specific rowkey; row_stop param is not inclusive
#   "SingleColumnValueFilter('cf', 'col', =, 'binary:value')"       rows conform to the condition; return all cols
#   "DependentColumnFilter('cf1','col1')"                           rows with cf:col
#   "ValueFilter(=,'binary:val1')"                                  specify value conform to the condition
#   "FamilyFilter(=,'binary:columnFamilyName')"                     specify column family name
#   "QualifierFilter(>,'binary:col1')"                              specify column name
# @return 返回一个迭代器，key为rowkey，value为包含所有列的字典
    def scanRows(self, tableName,\
                row_start=None, row_stop=None, row_prefix=None, columns=None,\
                filter=None, timestamp=None, include_timestamp=False,\
                batch_size=1000, scan_batching=None, limit=None,\
                sorted_columns=False, reverse=False):

        with self.hbase_pool.connection() as conn:
            table = conn.table(tableName)

            try:
                result = table.scan(row_start=row_start, row_stop=row_stop, row_prefix=row_prefix, columns=columns,\
                    filter=filter, timestamp=timestamp, include_timestamp=include_timestamp,\
                    batch_size=batch_size, scan_batching=scan_batching, limit=limit,\
                    sorted_columns=sorted_columns, reverse=reverse)
                return result
                logging.info("{}表 扫描数据成功".format(tableName))
            except BaseException:
                logging.info("{}表 扫描数据异常".format(tableName))
                exit()

#
# 删除数据
# @param tableName表名  rowKey: 需要删除的行 timestamp指定时间戳 wal是否写日志
#        columns: 包含所有要删除列的列表 ['columnFamily1:column1', 'columnFamily1:column2']
# delete one row:
#     table.delete('rowkey1')
# delete specific columnFamily of one row:
#     table.delete('rowkey1', columns=['columnFamily1'])
# delete specific column of one row:
#     table.delete('rowkey1', columns=['columnFamily1：column1', 'columnFamily1:column2'])
    def delete(self, tableName, rowKey, columns=None, timestamp=None, wal=True):
        with self.hbase_pool.connection() as conn:
            table = conn.table(tableName)
            try:
                table.delete(row=rowKey, columns=columns, timestamp=timestamp, wal=wal)
                logging.info("{}表 删除数据成功".format(tableName))
            except BaseException:
                logging.info("{}表 删除数据异常".format(tableName))
                exit()


if __name__ == "__main__":

    hbase = hbaseUtil(host='172.16.1.100', size=100, table_prefix='ORC_PRO')
    hbase.listTables()

    hbase.deleteTable('wh_table1')
    families={
        'cf1': dict(),
        'cf2': dict(),
        'cf3': {
                'max_versions': 3,
                'bloom_filter_vector_size': 0,
                'name': 'cf: ',
                'bloom_filter_type': 'NONE',
                'bloom_filter_nb_hashes': 0,
                'time_to_live': 2147483647,
                'in_memory': False,
                'block_cache_enabled': False,
                'compression': 'NONE'}
        }
    hbase.createTable('wh_table1', families)
    hbase.columnFamilyInfo('wh_table1')

    hbase.putOneCell('wh_table1', 'A1', 'cf1:col1', '+++++val1+++++')
    hbase.putOneCell('wh_table1', 'A5', 'cf1:col2', '+++++val2+++++')
    hbase.putOneCell('wh_table1', 'B2', 'cf1:col3', '+++++val3+++++')
    hbase.putOneCell('wh_table1', 'B3', 'cf1:col3', '-----val3-----v2')
    hbase.putOneCell('wh_table1', 'B222', 'cf1:col3', '-----val3-----v2')

    dt = {'cf1:col1': 'val1', 'cf1:col2': 'val2', 'cf2:col1': 'val3'}
    hbase.putOneRow('wh_table1', 'A2', dt)

    dt2 = {'C3': {'cf1:col1':'val1', 'cf3:col1':'val1'},
           'C33': {'cf1:col1':'val1', 'cf3:col1':'val1'}}
    hbase.putMultipleRows('wh_table1', dt2)

    scanner = hbase.scanRows('wh_table1')
    for r in scanner:
        print(r)

    # hbase.delete('wh_table1', '3', columns=['cf1:col1'])
    # print(hbase.getOneCell('wh_table1', '1', 'cf1:col3', versions=1))
