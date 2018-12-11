# TiKV engine
## 阿里云第一届数据性能大赛-初赛总结
只参加了初赛，复赛没时间参加。最终初赛结果top 59/1653，第一次参加这种性能比赛，收获颇丰。

![ali](https://upload-images.jianshu.io/upload_images/2720775-76be3555810d58e4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

### 一、题目
比赛总体分成了初赛和复赛两个阶段，整体要求实现一个简化、高效的 kv 存储引擎
初赛要求支持 Write、Read 接口。
复赛在初赛题目基础上，还需要额外实现一个 Range 接口。
程序评测逻辑 分为2个阶段：
 1）Recover 正确性评测：
此阶段评测程序会并发写入特定数据（key 8B、value 4KB）同时进行任意次 kill -9 来模拟进程意外退出（参赛引擎需要保证进程意外退出时数据持久化不丢失），接着重新打开 DB，调用 Read、Range 接口来进行正确性校验

 2）性能评测
随机写入：64 个线程并发随机写入，每个线程使用 Write 各写 100 万次随机数据（key 8B、value 4KB）
随机读取：64 个线程并发随机读取，每个线程各使用 Read 读取 100 万次随机数据
顺序读取：64 个线程并发顺序读取，每个线程各使用 Range 有序（增序）遍历全量数据 2 次 注： 2.2 阶段会对所有读取的 kv 校验是否匹配，如不通过则终止，评测失败； 2.3 阶段除了对迭代出来每条的 kv校 验是否匹配外，还会额外校验是否严格字典序递增，如不通过则终止，评测失败。


### 二、思路
#### 1.初步试水
第一次参加这种性能大赛，想着是先实现几种基本的kv模型,再调优。于是经过网上资料调研，先初步实现了几种基本的存储模型:BitCask、LSM。这时key,value是存在一起的，单独维护了索引文件，LSM树中采用超过阈值则进行compaction操作，即k路归并操作。

##### 设计要点：
- 内存中的Immutable MemTable：只能读不能写的内存表，为了不阻塞写操作，在一个MemTable满了的时候会将其设为Immutable MemTable并开启新的MemTable;
- 磁盘中的SSTable：存储的就是一系列的键值对;
- LSM Tree中分层: 使用双端队列进行MemTable的存放，这样在查找时可以遵循LRU原则，增加了内存hit，一定程度上优化了查找效率；
- 查询时增加了布隆过滤器以加速查找,在每个SSTable内进行二分查找;
- WAL：write ahead log：先写log，再写kv,以防止断电数据的丢失


##### 实现要点：
- 使用读写锁进行读写操作的同步;
- 使用synchronized同步tableCreationLock进行表创建的同步;
- 设置一些参数，比如每层鹅扇出数目，使得性能可调;


#### 2.经历挣扎
于是早早地写好了第一天开评测就线上测试，结果总是超时（官方限定一小时内没跑完则超时）。于是就沉迷优化代码，去掉重量级锁，改为轻量级锁，线下使用**JProfile**等工具进行性能调优。奈何无论如何调优，线上总是超时，偶尔还莫名其妙的OOM。经过几天debug，也没有任何起色。 

就在快要放弃时，经过一名大神提点，突然发现可能根本是我的方案就有问题。
才发现自己忽略了比赛很重要的一个前提：存储设备是SSD！而不是一般的磁盘。
于是再次调研，发现一篇论文：[WiscKey: Separating Keys from Values in SSD-conscious Storage](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) 是专门针对LSM在SSD上做的优化。核心思想就是将key 、value分开存储

并且了解到，本次比赛的 key 共计 6400w，分布比较均匀。于是经过和队友的讨论，觉得可以将key、value分开存,这可以解决写放大问题，但是线程竞争呢？64个线程，实际线下测的时候每次并发跑的最多50多个，总会有好几个线程被阻塞。于是查阅资料，发现可以使用“数据分桶”的思想，


#### 3.终于上路
于是，我们赶紧重新实现了一版BucketDB,按照分桶的思想，进行新的一轮测试，终于，这次有了姓名！

数据分桶的好处，可以大大减少顺序读写的锁冲突，而 key 的分布均匀这一特性，启发我们在做数据分区时，可以按照 key 的位进行设计hash函数，将高几位相同的key都分进一个桶。  

##### 设计要点：
- 将key、value 分开存，则key和value文件的write pos始终会保持一致，即索引只要存储key的file 的write pos就行了，也即当前file的长度
- key 分桶的hash 函数的设计：按照当前线程id，可以分到不同的桶中,根据 key hash，将随机写转换为对应分区文件的顺序写。


##### 实现要点：
- 将整个读写桶设计成BucketReaderWriter类，每个都维护了自己的data direct io file 和index mmap
- 数据key进行直接io，索引进行mmap，维护一个索引的mmap空间，不够的时候再去重新映射一段空间


### 三、优化
之后想到了几个优化思路：
1. 初始化加载内存映射时，为加快可以采取一大片内存映射mmap的方式
2. 初始化加载内存映射时，加载索引的消耗很大，考虑进行并行初始化，可以对64（或128）个Worker分别进行初始化，实测初始化耗时提升了60%
3. hash函数的优化：更改为 key[0] % CommonConfig.MAP_COUNT


### 四、遇到的坑
#### 1.线上各种OOM
- Heap ByteBuffer会有内存泄漏！！：  [Fixing Java's ByteBuffer native memory "leak"](http://www.evanjones.ca/java-bytebuffer-leak.html)
Heap bytebuffer会为每个线程都保持一个直接内存拷贝！
- ThreadLocal ====> 可能导致内存泄漏！！！
  ThreadLocal 通过隐式的在不同线程内创建独立实例副本避免了实例线程安全的问题
  对于已经不再被使用且已被回收的 ThreadLocal 对象，它在每个线程内对应的实例由于被线程的 ThreadLocalMap 的 Entry 强引用，无法被回收，可能会造成内存泄漏。

#### 2.内存调优
线上错误日志：内存映射文件时写入失败
后来发现是元空间的锅！jdk1.8里的metaspace!
元空间并不在虚拟机中，而是使用直接内存,
于是还需要设置一个Metaspace参数,以防止直接内存爆掉!
```
-XX:MaxMetaspaceSize=300m
```

另外，优化了整个程序的内存使用 => 随着整个程序的运行，内存占用越来越大，显然是很多对象没有回收 => 优化程序，通过将不必要的引用设为null，循环内不创建对象

#### 3.多线程竞争开销大
线上超时问题，一是LSM本身写放大明显；二是多线程竞争激烈，线程切换过多，导致写入性能急剧下降。 



### 代码地址：
[TiKV_engine](https://github.com/ZoeShaw101/TiKV_engine)

求star 求fork


### 参考文献
https://github.com/abbshr/abbshr.github.io/issues/58
http://www.cnblogs.com/haippy/archive/2011/12/04/2276064.html
https://raoxiaoman.github.io/2018/04/08/wisckey%E9%98%85%E8%AF%BB/. LSM-Tree存在的问题
https://colobu.com/2017/10/11/badger-a-performant-k-v-store/
https://www.cnblogs.com/fuzhe1989/p/7763612.html
https://zhuanlan.zhihu.com/p/38810568
http://blog.jobbole.com/69969/
https://www.zhihu.com/question/31024021
https://juejin.im/post/5ba9f9d7f265da0afd4b3de7 探究SSD写放大的成因与解决思路
http://codecapsule.com/2014/10/18/implementing-a-key-value-store-part-7-optimizing-data-structures-for-ssds/




## to-do
[ ] 实现分布式KV