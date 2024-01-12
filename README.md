# SecretFlow 隐私计算学习

## 文件说明

Secretflow，“隐语”隐私计算框架，支持包括 MPC、FL、TEE、HE、DP 在内的多种主流隐私计算技术，以期通过良好的分层设计及开箱即用的隐私保护数据分析及机器学习等功能，降低开发者使用门槛。

![image-20240112160548148](https://gitee.com/pan-canjie/blogimage/raw/master/img/202401121605367.png)

本仓库里的文件其实源于 Secretflow 官方文档的 Jupyter Notebook 学习教程。把它们单独拿出来整理成一个项目是因为，这些 Jupyter 源文件我都有亲手搭建环境运行起来，除了一些硬件资源限制和环境配置问题，所有文件都能正常跑通，它们对学习 Secretflow 这个框架有比较高的参考价值。

建议配合官方文档同步学习，效果更佳：[教程 | SecretFlow 文档 | 隐语 SecretFlow](https://www.secretflow.org.cn/docs/secretflow/latest/zh-Hans/tutorial)，把仓库里这些文件的代码逻辑、执行过程都理清楚，相信也就对 Secretflow 这个框架乃至包括安全多方计算、联邦学习和可信执行环境在内的隐私计算技术有深刻的理解了。接下里我会尝试基于 Secretflow，开发一些隐私计算相关的项目任务，希望在这个过程中与大家一同学习进步！

## 环境安装

根据官方文档的配置要求，我安装的环境是：

```python
python: 3.8	
pip: >= 19.3
OS: Ubuntu 22.04
CPU/Memory: 128C/64G	(A30 Cloud Server)
Platform: PyCharm Profession 2022.1.4
```

注：在云服务器上安装 docker 经常会出现问题，因为云服务器本身就是 docker 环境，因此选择 **Python Package Index 安装 SecretFlow**：

```py
pip install -U secretflow
```

安装后进行测试：

```python
>>> import secretflow as sf
>>> sf.init(parties=['alice', 'bob', 'carol'], address='local')
>>> alice_device = sf.PYU('alice')
>>> message_from_alice = alice_device(lambda x:x)("Hello World!")
>>> message_from_alice
<secretflow.device.device.pyu.PYUObject object at 0x7fdec24a15b0>
>>> print(sf.reveal(message_from_alice))
Hello World!

# 最后能打印出 “Hello World!”即是安装成功
```

## 报错说明

在学习的时候，你有可能在运行某些 demo 文件的时候出现和我一样的报错信息，特别是联邦学习那一部分：

![image-20240112163110032](https://gitee.com/pan-canjie/blogimage/raw/master/img/202401121631994.png)

这个问题困扰了我一段时间，好在通过与官方技术人员的交流，他们帮我解决了这个问题并解释了报错原因如下：初始化创建集群的过程其实也是资源声明的过程，如果没有配置 num_cpus 这个参数的话，会默认使用当前环境的 cpu 核数。这个 cpu 数量并不是真的占用的核数，而是 ray 能够调度的 tasks/actors 数量的上限。单机单 ray 集群的模拟运行时，这个数过小会出现没有可用资源的问题。因此资源声明、创建集群时，alice/bob/carol 可以声明大一点，这只是软限制，设成 32 或者更大一般不影响。例如：

```python
sf.init(['alice', 'bob', 'charlie'], address="local", log_to_driver=False, num_cpus=32
```

num_cpus 需要根据不同的环境条件分别配置。
