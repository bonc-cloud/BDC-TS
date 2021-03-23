# BDC-TS基准测试最佳实践 (CirroData-TimeS)
CirroData-TimeS是东方国信云上主推的时序数据库。接下来介绍如何使用CirroData-TimeS实现此BDC-TS的基准测试的最佳实践。

## 案例一：实时数据集测试

### 测试要求
测点数：60个指标*20,000辆车=1,200,000个测点  
数据生成间隔：1s（每个测点每隔1s产生一条数据，保证每秒有120万条数据生成）   
方式：直接调用数据库接口写入 

### 测试实现步骤
#### 1、产生数据
use-case：这里使用的vehicle，也就是BDC-TS标准，请不要修改  
scalevar：定义有多少个设备同时上报，这个案例中约定20000个vehicle同时上报数据，所以是20000，请不要修改  
format： 写cirrotimes-bulk、influx-bulk、opentsdb等，根据实际填入  
timestamp-start：数据开始时间 格式诸如 2008-01-01T08:00:01Z  
timestamp-end：数据结束时间 格式诸如 2008-01-01T08:00:01Z  
sg-num：CirroData-TimeS中存储组的数量
  
如，20000个设备产生1秒的数据应该使用以下命令  
```powershell
$GOPATH/bin/bulk_data_gen --seed=123 --use-case=vehicle --scale-var=20000 --format=cirrotimes-bulk --sg-num 20 --timestamp-start=2008-01-01T08:00:00Z --timestamp-end=2008-01-01T08:00:01Z > cirrotimes_bulk_vehicle__scalevar_20000_seed_123
```  

#### 2、导入数据到数据库
host：CirroData-TimeS服务对应的ip  
port: CirroData-TimeS服务对应的port  
tablets-batch： CirroData-TimeS执行insert tablets时候对应的tablet的数量  

调用下面命令，写入数据到数据库
```powershell
cat cirrotimes_bulk_vehicle__scalevar_20000_seed_123 | ./bulk_load_cirrotimes_tablets -batch-size 50000 -workers 20 -host 172.16.48.8 -port 6667 --tablets-batch 1000
``` 

导入速度结果会如下显示：
```powershell
loaded 20160 items in 4.125944sec with 2 workers (mean values rate 216282.663904/sec)
```

#### 3、结果汇总
汇总上面的结果日志 TODO

  
  
## 案例二：历史数据集1（测点少）
### 测试要求
数据生成在一个csv文件中，数据总量约1TB  
测点数：60个指标*20辆车=1,200个测点  
数据生成间隔：N（每个测点每隔Ns时间产生一条数据，数据周期持续1年）
### 测试实现步骤
#### 1、产生数据
```powershell
$GOPATH/bin/bulk_data_gen --seed=123 sampling-interval 30s --use-case=vehicle --scale-var=20 --format=cirrotimes-bulk --sg-num 20 --timestamp-start=2008-01-01T08:00:00Z --timestamp-end=2009-01-01T08:00:00Z > cirrotimes_bulk_vehicle_scalevar_20_seed_123
``` 

#### 2、导入数据到数据库
host：CirroData-TimeS服务对应的ip  
port: CirroData-TimeS服务对应的port  
tablets-batch： CirroData-TimeS执行insert tablets时候对应的tablet的数量  

调用下面命令，写入数据到数据库  
```powershell
cat cirrotimes_bulk_vehicle__scalevar_20_seed_123 | ./bulk_load_cirrotimes_tablets -batch-size 50000 -workers 20 -host 172.16.48.8 -port 6667 --tablets-batch 1000
```
导入速度结果会如下显示：
```powershell
loaded 20160 items in 4.125944sec with 2 workers (mean value rate 298055.391939/s)
```
#### 3、结果汇总
汇总上面的结果日志 TODO
  
  
## 案例三：历史数据集2（测点多）
### 测试要求
数据生成在一个csv文件中，数据总量约1TB  
测点数：60个指标*20,000辆车=1,200,000个测点  
数据生成间隔：1s（每个测点每隔1s产生一条数据，数据周期持续M时间）
### 测试实现步骤
#### 1、产生数据
```powershell
$GOPATH/bin/bulk_data_gen --seed=123 sampling-interval 1s --use-case=vehicle --scale-var=20000 --format=cirrotimes-bulk --sg-num 20 --timestamp-start=2008-01-01T08:00:30Z --timestamp-end=2009-01-01T08:00:00Z > cirrotimes_bulk_vehicle_scalevar_20000_seed_123
```
#### 2、导入数据到数据库
host：CirroData-TimeS服务对应的ip  
port: CirroData-TimeS服务对应的port  
tablets-batch： CirroData-TimeS执行insert tablets时候对应的tablet的数量  

调用下面命令，写入数据到数据库  
```powershell
cat cirrotimes_bulk_vehicle__scalevar_20000_seed_123 | ./bulk_load_cirrotimes_tablets -batch-size 50000 -workers 20 -host 172.16.48.8 -port 6667 --tablets-batch 1000
```
导入速度结果会如下显示：
```powershell
loaded 20160 items in 4.125944sec with 2 workers (mean value rate 298055.391939/s)
```
#### 3、结果汇总
汇总上面的结果日志 TODO