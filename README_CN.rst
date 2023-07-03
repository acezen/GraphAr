GraphAr
=======

|GraphAr CI| |Docs CI| |GraphAr Docs| |Good First Issue|

欢迎来到 GraphAr（Graph Archive 的简称），一个用于图数据存储和检索的开源、标准化文件格式。

GraphAr 是什么？
----------------

|

.. image:: https://alibaba.github.io/GraphAr/_images/overview.png
  :width: 770
  :align: center
  :alt: Overview

|

图处理是各种现实世界应用程序的基本构建块，例如社交网络分析、数据挖掘、网络路由和科学计算。
GraphAr 是一个旨在为不同的应用程序和系统（内存和分布式存储、数据库、图形计算系统和交互式图形查询框架）提供便利、高效地构建和访问图形数据的项目。
它可用于导入/导出和持久存储图形数据，从而降低系统之间协作时的负担。此外，它还可以作为图形处理应用程序的直接数据源。

为了实现这一目标，GraphAr 提供了以下功能：

- 图归档（GAR）文件格式：一种标准化的、与系统无关的文件格式，用于存储图形数据。
- 库：一组库，用于读取、写入和转换 GAR 文件。

使用 GraphAr，你可以：

- 使用 GAR 文件格式以系统无关的方式存储和持久化图形数据。
- 使用库轻松访问和生成 GAR 文件。
- 利用 Apache Spark 快速操作和转换 GAR 文件。

GAR 文件格式
-----------
GAR 文件格式旨在用于存储属性图。它使用元数据记录所有必要的图形信息，并以分块的方式维护实际数据。

属性图包含顶点和边，每个顶点都包含一个唯一标识符和：

- 一个描述顶点类型的文本标签。
- 一组属性，每个属性都有一个名称和一个值。

每条边都包含一个唯一标识符和：

- 一个描述边类型的文本标签。
- 一个源顶点标识符。
- 一个目标顶点标识符。
- 一组属性，每个属性都有一个名称和一个值。

以下是一个包含两种顶点类型（“person”和“comment”）和三种边类型的属性图示例。

.. image:: https://alibaba.github.io/GraphAr/_images/property_graph.png
  :width: 700
  :align: center
  :alt: property graph


GraphAr 中的顶点
---------------

逻辑点表
^^^^^^^^^

每种类型的顶点（具有相同的标签）构成一个逻辑顶点表格，其中每个顶点被分配一个全局索引（称为内部顶点 ID），从0开始，对应于顶点在逻辑顶点表格中的行号。提供了一个以“person”


.. _Apache License 2.0: https://github.com/alibaba/GraphAr/blob/main/LICENSE

.. |GraphAr CI| image:: https://github.com/alibaba/GraphAr/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/alibaba/GraphAr/actions

.. |Docs CI| image:: https://github.com/alibaba/GraphAr/actions/workflows/docs.yml/badge.svg
   :target: https://github.com/alibaba/GraphAr/actions

.. |GraphAr Docs| image:: https://img.shields.io/badge/docs-latest-brightgreen.svg
   :target: https://alibaba.github.io/GraphAr/

.. |Good First Issue| image:: https://img.shields.io/github/labels/alibaba/GraphAr/Good%20First%20Issue?color=green&label=Contribute%20&style=plastic
   :target: https://github.com/alibaba/GraphAr/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22

.. _GraphAr File Format: https://alibaba.github.io/GraphAr/user-guide/file-format.html

.. _GraphAr Spark Library: https://github.com/alibaba/GraphAr/tree/main/spark

.. _GraphAr C++ Library: https://github.com/alibaba/GraphAr/tree/main/cpp

.. _example files: https://github.com/GraphScope/gar-test/blob/main/ldbc_sample/

.. _contribution guidelines: https://github.com/alibaba/GraphAr/tree/main/CONTRIBUTING.rst

.. _Code of Conduct: https://github.com/alibaba/GraphAr/blob/main/CODE_OF_CONDUCT.md

.. _GraphAr Slack: https://join.slack.com/t/grapharworkspace/shared_invite/zt-1wh5vo828-yxs0MlXYBPBBNvjOGhL4kQ

.. _GraphAr Weekly Community Meeting: https://github.com/alibaba/GraphAr/wiki/GraphAr-Weekly-Community-Meeting

.. _community introduction: https://github.com/alibaba/GraphAr/tree/main/docs/developers/community.rst

.. _GitHub Issues: https://github.com/alibaba/GraphAr/issues/new

.. _Github Discussions: https://github.com/alibaba/GraphAr/discussions
