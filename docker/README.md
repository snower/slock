# Docker配置

### 构建

```bash
docker build .
```

### 启动

```bash
docker run -d --name slock -p 5658:5658 slock

#运行时可用环境变量“ARG_{命令行参数名大写}”指定启动命令行参数
#如指定日志输出文件名称（默认日志输出到控制台）

docker run -d --name slock -p 5658:5658 -e ARG_LOG=/slock/slock.log slock
```