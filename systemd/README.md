# systemd 配置

- 添加系统用户

```bash
useradd -M -s /bin/nologin -U slock
```

- 复制配置文件到系统配置目录

```bash
cp slock.toml /etc/
```

- 添加数据目录、日志文件并修改权限

```bash
mkdir /var/lib/slock
mkdir /var/log/slock
touch /var/log/slock/slock.log

chown -R slock:slock /var/lib/slock
chown -R slock:slock /var/log/slock
```

- 复制systemd配置文件

```bash
cp slock.service /etc/systemd/user/
```

- systemd重写加载及启动

```bash
systemctl daemon-reload
systemctl enable slock
systemctl start slock
```