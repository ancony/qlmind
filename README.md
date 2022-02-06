# HQL转为XMIND qlmind

## 主要功能

将hql转为以table为中心的思维导图

- 支持单个hql语句。
- 支持以sql为扩展名的文件。
- 支持目录。

## 不支持的hql语法

对以下语法暂不支持

- where中包含子查询
- 表含有分区信息
- 多重插入

## 解析原理

将语句通过hive的语法解析器解析为AST后，对AST进行重新包装，
与table相关的节点转为ITopic对象。不相关的部分重新拼接为字符串。

由于并未测试完所有的可能组合，可能存在解析过程中报错的情况，
如有报错，可以反馈给我。

## 问题反馈
