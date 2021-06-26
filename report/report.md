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

#### 对外接口

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

#### 模块测试

```c++
#include "BufferManager.hpp"

void BufferManagerTest() {
  auto a = new Block;
  memset(a->val_, 0x3f, sizeof(a->val_));
  const auto a_id = buffer_manager.Create(a);
  a->pin_ = true;
  a->dirty_ = true;
  for (int i = 0; i < Config::kMaxBlockNum; i++) {
    auto b = new Block;
    buffer_manager.Create(b);
  }
  a->pin_ = false;
  auto b = new Block;
  buffer_manager.Create(b);
  auto c = buffer_manager.Read(a_id);
}

```
通过上述代码测试基本的 read/write, dirty/pin 的功能，为了方便查看，使用 gdb 以及 hexdump 观察结果而不是输出到 console 里面。测试是通过修改 Config 明明空间下的参数调整 buffer 的大小测试不同情况。并行输出部分的性能测试不在此处进行，而是在整个程序对接完毕之后进行。

### Catalog Manager 模块

#### 对外接口

正如**模块设计与简要概述**中所说，Catalog Manager 的主要功能是管理表格及其信息。所以其对外接口为 `CreateTable`, `DropTable`, `CreateIndex`, `DropIndex`, `TableInfo` 等。其作用与名字一致，即在检查语法和语义正确性之后对数据库模式信息进行修改。在此不做赘述。

#### 实现细节

* Catalog Manager 在构造函数中读取了磁盘中存下的数据库模式信息，实现了初始化。在析构函数中将数据库模式信息写回，保证了数据安全性。
* Catalog Manager 管理的模式信息由于经常读写且长度不一定在一个 block 范围内，所以不由 Buffer Manager 进行管理。它自己直接开设了一个文件实现了信息的永久保存。
* 为了提高性能，使用了 std::unordered_map 来保存了表名与表信息之间的映射。

#### 模块测试

```mysql
create table student (
        sno char(8),
        sname char(16) unique,
        sage int,
        sgender char (1),
        score float,
        primary key ( sno )
);
create index stunameidx on student ( sname );
drop index stunameidx on student;
drop table student;

```
通过上述代码测试基本的 create/drop table/index 的功能，为了方便查看，使用 gdb 以及 hexdump 观察结果而不是输出到 console 里面。

### Index Manager 模块

### Interpreter 模块

#### 对外接口

```c++
void interpret();

bool interpretFile(const std::filesystem::path &filename);

void setWorkdir(const std::filesystem::path &dir);
```

其中：
* `interpret` 从 `stdin` 接受输入，并进行解析和后续执行
* `interpretFile` 打开由 `filename` 指定的文件，并进行解析和后续执行
* `setWorkdir` 设置当前 Interpreter 的工作目录，进而和 `interpretFile` 和 `execfile` 良好配合，实现对相对路径的正确解释及嵌套 `execfile` 命令的处理

#### 实现细节

* Interpreter 是手工编写的递归下降无回溯LL(1)语法分析器。对于MiniSQL文法中的每一个非终结符，Interpreter 都使用了一个对应的函数(parse*)进行解析。这些函数相互调用，从而实现对用户输入的解析。由于 MiniSQL 的文法得到了很大简化，因此无需构造 AST，只需记录对应表名、索引名、条件集或元组集即可。若发生 Token 不匹配、字符串长度超限等情况，Interpreter 给出语法错误信息并终止此语句执行。在完成 Statement 层次的非终结符解析后，Interpreter会根据需要，决定是否从 Catalog Manager 请求相应的信息对用户输入的一部分进行检查。检查完成后，则会调用 API 层，执行语句对应的语义动作，并统计时间，按需输出结果。

### Record Manager 模块

#### 对外接口

Record Manager 提供了在 Block 抽象之上的记录操作。主要的对外接口包括有：

```c++
bool createTable(const Table& table);

bool dropTable(const Table& table);

Position insertRecord(const Table& table, const Tuple& tuple);

vector<Tuple> selectAllRecords(const Table& table);

vector<Tuple> selectRecord(const Table& table,
                            const vector<Condition>& conds);

size_t deleteRecord(const Table& table, const vector<Condition>& conds);

size_t deleteAllRecords(const Table& table);

RecordAccessProxy getIterator(const Table &table);
```

此外还对外还开放了记录迭代器的对应接口，在此不作过多赘述

其中：
* `createTable` 接受 Catalog Manager 传递的 table 信息，调用 Buffer Manager 为该表分配块，并准备记录访问迭代器
* `dropTable` 接受 Catalog Manager 传递的 table 信息，根据存储的表占用块信息，依次遍历，清空对应块的所有记录，并删除对应的占用块信息
* `insertRecord` 接受 Catalog Manager 传递的 table 信息，使用新的或缓存的记录访问迭代器，遍历各表所占用块中的槽位，直到遇见第一个空槽位并插入
* `selectAllRecords` 接受 table 信息，从头开始访问表的所有存储块，若槽位中存有数据则将其加入结果数组。这是 `selectRecord` 的优化版本
* `selectRecord` 接受 table 信息，从头开始访问表的所有存储块，若槽位中存有数据，且满足所有查询条件，则将其加入结果数组
* `deleteAllRecords` 接受 table 信息，从头开始访问表的所有存储块，若槽位中存有数据则将其无效化。这是 `deleteRecord` 的优化版本
* `deleteRecord` 接受 table 信息，从头开始访问表的所有存储块，若槽位中存有数据，且满足所有查询条件，则将其无效化
* `getIterator` 接受 table 信息，暴露一个从头开始的记录访问迭代器以供其他组件使用

#### 实现细节

* Record Manager 在构造函数中读取了磁盘中存下的表占用块信息，实现初始化。在析构函数中将表占用块信息写回，保证了数据安全性。由于表-块对应关系的数据频繁访问且可能跨 block，因此单独存储于文件中。
* Record Manager 实现了单块无序多记录存储。Record Manager 借助 RecordAccessProxy 所提供的接口，可以遍历块上的所有可能的数据存储槽位，并调用接口来判断此处是否存有有效数据。在条件查询/删除的情境下，在判断有效性后，Record Manager还会进一步判断该记录是否满足条件。
* RecordAccessProxy封装了低级块操作。RecordAccessProxy 可以根据 Catalog Manager 提供的记录长度信息，递增数据指针指向下一条记录。若修改过当前块，则对 block 置 dirty_ 位以通知 Buffer Manager 择机回写该块。 当指针即将指出当前 block 时，RecordAccessProxy 会查找下一有效 block，并且修改 block 指针、设置新块和旧块的 pin 状态。若已到达最后一块的结尾时，则会返回失败状态。可以通过 RAP 的接口来调用 Buffer Manager 来分配一个新的空闲块，以供插入。此外，RAP还提供了元组提取、块位置提取接口以供其他组件使用
  
## 程序展示

## 总结
