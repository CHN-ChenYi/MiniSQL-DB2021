## 实验要求

> 设计并实现一个精简型单用户 SQL 引擎(DBMS) MiniSQL，允许用户通过字符界面输入输入 SQL 语句实现表的建立/删除；索引的建立/删除以及表记录的插入/删除/查找。
>
> 通过对 MiniSQL 的设计与实现，提高学生的系统编程能力，加深对数据库系统原理的理解。

### 需求概述

> * 数据类型
>
>   只要求支持三种基本数据类型：int，char(n)，float，其中 char(n) 满足： 1 <= n <= 255。
>
> * 表定义
>
>   一个表最多可以定义32个属性，各属性可以指定是否为 unique；支持单属性的主键定义。
>
> * 索引的建立和删除
>
>   对于表的主属性自动建立 B+ 树索引，对于声明为 unique 的属性可以通过 SQL 语句由用户指定建立/删除 B+ 树索引（因此，所有的 B+ 树索引都是单属性单值的）。
>
> * 查找记录
>
>   可以通过指定用 and 连接的多个条件进行查询，支持等值查询和区间查询。
>
> * 插入和删除记录
>
>   支持每次一条记录的插入操作；支持每次一条或多条记录的删除操作。

## 总体设计

### 组员分工

<!-- Git 版本隐去此信息，自己看 git log 去（逃 -->

### 模块设计与简要概述

![](./img/overall.png)

*与实验指导书的设计略有不同，我们的模块设计中 Catalog Manager 不直接与 Interpreter 交互，而是通过 API 层进行中转。*

#### Interpreter

直接与用户进行交互，接收并结构化用户输入的命令，检测语法正确性后调用 API 层函数执行命令。

#### API

提供各个 SQL 语句的执行接口，供 Interpreter 层调用。在检验语义正确性后通过调用各个模块的接口执行命令。

#### Catalog Manager

负责管理数据库的所有模式信息，包括表的名称、字段、主键、索引等。

#### Record Manager

管理和记录表中数据，提供记录的插入、删除和查找等操作接口。保证 unique 等语义正确性。

#### Index Manager

由 B+ 树实现索引，提高命令运行速度。

#### Buffer Manager

负责缓冲区的管理。
1. 根据需要，读取指定的数据到系统缓冲区或将缓冲区中的数据写出到文件
2. 实现缓冲区的替换算法，当缓冲区满时根据 LRU 模式进行替换
3. 记录缓冲区中各页的状态
4. 提供缓冲区页的 pin 功能，防止正在被操作的页被换出

#### DB Files

指构成数据库的所有数据文件，包括记录数据、索引数据及 Catalog 数据等。

### 基本流程

<!-- TODO -->

## 详细设计

### API 模块

### Buffer Manager 模块

Buffer Manager 提供了基本的 Block 抽象。主要对外接口为以下三个：

``` c++
Block *Read(const size_t &block_id);

size_t Create(Block *block);

size_t NextId();
```

其中：

* `Read` 通过 block_id 取回对应的 block，如果 block 在内存中，则直接取回。如果不在，则从磁盘中读入 block，此时如果 buffer 已经满了，则找到未被访问时间最久的没有被 pin 住的 block，将其写回磁盘并换出 buffer。
* `Create` 将别的模块生成的新的 block 加入 buffer 中，若 buffer 已满，则与 `Read` 行为一致。
* `NextId` 返回下一个 `Create` 操作会返回的 block_id。

#### 实现细节

* Buffer Manager 在构造函数中读取了磁盘中最大的 block_id，实现了初始化。在析构函数中将所有还在 buffer 中的脏 block 全部写回磁盘，保证了数据安全性。
* Buffer Manager 有三个版本，通过编译选项进行开关：
  * 基础版本为裸实现。即文件写操作在主线程中执行。
  * 第二个版本通过 c++ 的 std::future 以及 std::async 实现了异步非阻塞的文件写操作，从而实现了数据库运算与 IO 并行，一定程度上优化了性能。（在 CMake 中通过 ParallelWrite 选项默认打开）
  * 第三个版本通过 std::mutex, std::atomic, std::condition_variable 等实现了一个线程池，在 MiniSQL 程序打开时默认新建（机器核数 - 1）个线程用于磁盘写操作。规避了在第二个版本中线程生成与销毁的开销，进一步优化了性能。（在 TaskPool.hpp 中通过宏定义 UseThreadPool 默认打开）

### Catalog Manager 模块

### Index Manager 模块

### Interpreter 模块

### Record Manager 模块

## 程序展示

## 总结
