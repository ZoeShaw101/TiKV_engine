# TiKV engine
阿里云第一届数据性能大赛-初赛代码，一个可扩展KV engine库， java代码在engine_java下面

## 模型
#### BitCask
基于哈希的存储引擎BitCask

#### LSM Tree
日志结构合并树

LSM基本结构：
- 内存中的MemTable
- 内存中的Immutable MemTable：只能读不能写的内存表
- 磁盘中的SSTable：存储的就是一系列的键值对
- Bloom Filter：提高Key在不在SSTable的判定速度

#### Bucket
针对SSD硬盘进行的优化模型，LSM在SSD上写放大缺点很明显。
采用分桶的思想，key value分开存储。每个线程一个桶。

## 性能对比 

## to-do
[ ] 实现分布式KV