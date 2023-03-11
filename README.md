# pipe query 

## 项目描述

用sql 来过滤、处理数据流

如： 日志分析、文件分析、文件过滤等等

可以作为 awk 的一种补充，或者你能想象的别的用处

## 本项目目的

1. 为学习sql,更深入了解sql以及其运行原理
2. 为文本处理多增一个工具


## 使用例子

直接写条件


```
ls -l | pq  -w "c5>1024"

```

也可以写完整的sql语句:

```
ls -l |pq -e "select * from stdin where c5>1024"

```

- [x] 支持嵌套查询：

```
ls -l |./pq -e ' select * from (select * from stdin)'
```

更多例子，参考单元测试

## 数据源

| 是否支持 | 数据源 | 描述 |
|--| -- | -- |
|是|stdin|  标准输入 |
||file|  文件输入 |
||kafka|  kafka队列输入 |
||mysql|  从mysql输入 |

