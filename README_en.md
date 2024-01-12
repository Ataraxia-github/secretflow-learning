# SecretFlow Learning
## File Description
Secretflow, a privacy computing framework, supports a variety of mainstream privacy computing technologies including MPC, FL, TEE, HE, and DP, in order to achieve privacy-preserving data analysis and machine learning through good layered design and out-of-the-box and other functions to lower the threshold for developers to use.
![image-20240112160548148](https://gitee.com/pan-canjie/blogimage/raw/master/img/202401121605367.png)

The files in this repository actually originate from the Jupyter Notebook learning tutorial of Secretflow official documentation. The reason why I took them out separately and organized them into one project is that I have personally set up the environment to run these Jupyter source files. Except for some hardware resource limitations and environment configuration issues, all files can run normally. They are useful for learning the secretflow framework. Relatively high reference value.

It is recommended to study synchronously with the official documents for better results: [Tutorial | SecretFlow Documentation | Hidden SecretFlow](https://www.secretflow.org.cn/docs/secretflow/latest/zh-Hans/tutorial), put these in the warehouse Once you have clarified the code logic and execution process of the file, I believe you will have a deep understanding of the Secretflow framework and even privacy computing technologies including secure multi-party computation, federated learning, and trusted execution environments. Next, I will try to develop some project tasks related to privacy computing based on Secretflow. I hope to learn and make progress with everyone in the process!
## Environment Installation
According to the configuration requirements of the official documentation, the environment I installed is:
```python
python: 3.8	
pip: >= 19.3
OS: Ubuntu 22.04
CPU/Memory: 128C/64G	(A30 Cloud Server)
Platform: PyCharm Profession 2022.1.4
```

Note: Problems often occur when installing docker on a cloud server, because the cloud server itself is a docker environment, so choose **Python Package Index to install SecretFlow**:
```py
pip install -U secretflow
```
Test after installation:


```python
>>> import secretflow as sf
>>> sf.init(parties=['alice', 'bob', 'carol'], address='local')
>>> alice_device = sf.PYU('alice')
>>> message_from_alice = alice_device(lambda x:x)("Hello World!")
>>> message_from_alice
<secretflow.device.device.pyu.PYUObject object at 0x7fdec24a15b0>
>>> print(sf.reveal(message_from_alice))
Hello World!

# If "Hello World!" is printed out at the end, the installation is successful.
```
## Error Description
When learning, you may get the same error message as me when running some demo files, especially the Federated Learning part:
![image-20240112163110032](https://gitee.com/pan-canjie/blogimage/raw/master/img/202401121631994.png)
This problem has troubled me for a while. Fortunately, through communication with official technical staff, they helped me solve this problem and explained the reason for the error as follows: The process of initializing and creating a cluster is actually the process of resource declaration. If the parameter num_cpus is not configured If so, the CPU core number of the current environment will be used by default. This number of CPUs is not the actual number of cores occupied, but the upper limit of the number of tasks/actors that ray can schedule. When the simulation of a single machine and a single ray cluster is run, if this number is too small, there will be a problem of no available resources. Therefore, when declaring resources and creating a cluster, alice/bob/carol can be declared larger. This is only a soft limit. Setting it to 32 or larger generally has no effect. For example:
```python
sf.init(['alice', 'bob', 'charlie'], address="local", log_to_driver=False, num_cpus=32
```
num_cpus needs to be configured separately according to different environmental conditions.
