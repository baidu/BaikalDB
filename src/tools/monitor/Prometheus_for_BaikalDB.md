## 一、概述

本文档主要用于BaikalDB指标接入Prometheus监控平台。

Prometheus是一个系统和服务监控系统。它以给定的时间间隔从已配置的目标收集指标，评估规则表达式，显示结果，并在观察到指定条件时触发警报。

## 二、工作流程

1. 打开bvar的dump功能，运行BaikalDB，monitor目录下会有监控指标产生。
2. 将BaikalDB产生的监控指标导出到Prometheus。
3. 安装Grafana可视化BaikalDB产生的监控指标。
4. 安装Alertmanager配置报警规则。

## 三、具体操作

### 1. 打开bvar的dump功能

在BaikalDB的配置文件中添加如下配置(以**db配置**为例)：

```
-bvar_dump
-bvar_dump_file=./monitor/bvar.baikaldb.data
```

对于meta和store，bvar_dump_file分别为 **./monitor/bvar.baikalMeta.data** 和 **./monitor/bvar.baikalStore.data**

打开dump功能之后，**检查 monitor 目录下是否有监控数据产生**。

### 2. 安装并启动Prometheus

在 https://prometheus.io/download/ 中下载Prometheus文件及alertmanager文件。本文档下载 prometheus-2.23.0.linux-amd64.tar.gz。

修改 prometheus.yml 配置文件(配置文件可以参考)，其中：

* scrape_interval 代表刮取间隔，默认为15s
* evaluation_interval 代表报警评估间隔，默认为15s
* alerting 为报警相关配置
* rule_files 为报警规则文件
* scrape_configs 为刮取目标相关配置 **(metrics_path 设置为 /brpc_metrics)**

```
$ tar -zxvf prometheus-2.23.0.linux-amd64.tar.gz
$ cd prometheus-2.23.0.linux-amd64
$ vim prometheus.yml

# 这个配置文件可供参考
# my global config
global:
  scrape_interval:     15s # Set the scrape interval to every 15 seconds. Default is every 1 minute.
  evaluation_interval: 15s # Evaluate rules every 15 seconds. The default is every 1 minute.
  # scrape_timeout is set to the global default (10s).

# Alertmanager configuration
alerting:
  alertmanagers:
  - static_configs:
    - targets:
      # - alertmanager:9093

# Load rules once and periodically evaluate them according to the global 'evaluation_interval'.
rule_files:
  # - "first_rules.yml"
  # - "second_rules.yml"

# A scrape configuration containing exactly one endpoint to scrape:
# Here it's Prometheus itself.
scrape_configs:
 # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: 'meta'
    metrics_path: '/brpc_metrics'
    static_configs:
    - targets: ['xx.xx.xx.xx:8010']
    
  - job_name: 'db'
    metrics_path: '/brpc_metrics'
    static_configs:
    - targets: ['xx.xx.xx.xx:8888']

  - job_name: 'store'
    metrics_path: '/brpc_metrics'
    static_configs:
    - targets: ['xx.xx.xx.xx:8110']


# 启动Prometheus
$ nohup ./prometheus --config.file=prometheus.yml &
```

Prometheus启动之后，访问 [http://localhost:9090](http://localhost:9090) 可以Prometheus网页，在 http://xx.xx.xx.xx:9090/metrics 中可以看到刮取的监控指标。

![image-20201210114314622](https://github.com/lvxinup/Prometheus_for_BaikalDB/raw/main/image-20201210114314622.png)

### 3. 安装并启动Grafana可视化监控指标

在 https://grafana.com/get 中下载Grafana。这里我们以 grafana-7.3.4.linux-amd64.tar.gz 为例。

```
$ tar -zxvf grafana-7.3.4.linux-amd64.tar.gz
$ cd grafana-7.3.4
# 启动Grafana
$ nohup ./bin/grafana-server web &
```

启动Grafana之后，请打开浏览器并转到 [http:// localhost:3000 /](http://localhost:3000/)。首次登陆的用户名密码均为 admin。

* 点击侧边栏Configuration，之后点击 Add data source，选择 Time series databases 为 Prometheus。
* 在数据源配置中，URL填写Prometheus的启动IP+端口，这里为 xx.xx.xx.xx:9090，保存数据源。
* 点击侧边栏 Create Dashboard，开始配置面板。
* 点击 Add new panels，在Metrics中输入表达式 ，Settings中Panel title修改面板名。
* 表达式可以在 http://xx.xx.xx.xx:8888/vars 中选择(meta端口为8010，store端口为8110)。
* 这里表达式以 select_time_cost_qps 和 dml_time_cost_qps 为例。

### 4. 安装并启动Alertmanager

之前步骤中已经下载 Alertmanager文件，这里以 alertmanager-0.21.0.linux-amd64 为例。

##### 在prometheus下添加 rules.yml文件

示例设置报警为select qps大于5触发报警，实际报警规则根据需要设置。

```
$ cd prometheus-2.23.0.linux-amd64
$ vim rules.yml

groups:
- name: hostStatsAlert
  rules:
  - alert: hostQPSAlert
    expr: (select_test_qps) > 5
    for: 1m
    labels:
      severity: page
    annotations:
      summary: "Instance {{ $labels.instance }} QPS high"
      description: "{{ $labels.instance }} QPS above 5 (current value: {{ $value }})"
```

##### 修改 prometheus.yml 文件

```
$ vim prometheus.yml

# 修改以下配置即可
alerting:
  alertmanagers:
  - static_configs:
    - targets: ['xx.xx.xx.xx:9093']

# Load rules once and periodically evaluate them according to the global 'evaluation_interval'.
rule_files:
  - "rules.yml"


# 修改Prometheus配置文件后要重启Prometheus。
$ nohup ./prometheus --config.file=prometheus.yml &
```

##### 修改 AlertManager 配置文件

报警这里以邮件报警为例，QQ邮箱需要开启smtp，设置->账户->开启服务。

开启POP3/SMTP服务与IMAP/SMTP服务会**生成授权码**。

global为发送邮件的账户，其中 smtp_from 与 smtp_auth_username 均为邮箱，smtp_auth_password填写授权码。

receivers为接收邮件的账户信息，在 email_configs->to中填写收件账户。

```
$ tar -zxvf alertmanager-0.21.0.linux-amd64.tar.gz
$ cd alertmanager-0.21.0.linux-amd64
$ vim alertmanager.yml

global:
  resolve_timeout: 5m
  smtp_smarthost: 'smtp.qq.com:587'
  smtp_from: 'xxxx@qq.com'
  smtp_auth_username: 'xxxx@qq.com'
  smtp_auth_password: 'xxxxxxxxxxxx'
  smtp_require_tls: false
  smtp_hello: 'qq.com'

route:
  group_by: ['alertname']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 1h
  receiver: 'mail'
receivers:
- name: 'mail'
  email_configs:
  - to: 'xxxx@163.com'
    send_resolved: true
inhibit_rules:
  - source_match:
      severity: 'critical'
    target_match:
      severity: 'warning'
    equal: ['alertname', 'dev', 'instance']
    
# 启动 AlertManager
$ nohup ./alertmanager &
```

Alertmanager启动之后，多次select即可触发报警。

![image-20201210151400987](https://github.com/lvxinup/Prometheus_for_BaikalDB/raw/main/image-20201210151400987.png)

