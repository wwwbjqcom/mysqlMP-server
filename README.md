
  
    
# mysqlMP-server      
 mysql高可用管理      
      
      
# 功能      
 1. 主从高可用管理    
 2. 手动切换主从关系    
 3. 节点维护模式       
 4. 宕机差异数据追加、回滚    
 5. 差异数据人工审核是否恢复    
 6. 宕机自动恢复主从关系    
 7. 自动管理读写路由关系    
 8. 慢日志管理（开发中）    
 9. sql审计管理 （开发中）  
 10. mysql运行状态监控   
      
      
## 使用方法： 下载源码进行编译，或者下载最新release下的可执行文件。有两个参数项:    
1. listen  监听地址，默认:127.0.0.1     
 2. port    监听端口， 默认8099    
    
同时下载web项目， 把可执行文件放于web目录直接运行即可，然后就可以在浏览器输入https://127.0.0.1:8099进行操作，初始用户名密码为admin/admin。    
    
添加的数据库节点需配合[mysqlMP-client](https://github.com/wwwbjqcom/mysqlMP-client)使用。    
      
### 读写路由获取: 读写路由关系通过api接口的方式获取，例如我使用py进行获取，方法如下：    
    

>  import requests,json    
>  url = 'https://127.0.0.1:8099/getrouteinfo'  
> d = {'hook_id':'w2OLkdO212qs6zXzlAWj0P8rzYKa4PxZ', 'clusters': ['test']}      
> r = requests.post(url, data=json.dumps(d), headers={'Content-Type': 'application/json'},verify=False)      
> print(r.text)

clusters: 为集群名列表， 可以同时获取多个    
hook_id: 登陆web页面后在用户信息处获取到    
      
### 注意事项:    
1. 主从复制只支持gtid模式，不支持binlog+position的方式   
2. 仅支持master-slave管理
3.  不支持多通道复制    
4. slave默认都会设置为read_only 及双0刷盘配置   
5. 所有节点都有参与master宕机复检，所以节点之间对应端口需要互通及mysql登陆权限     
6. slave节点宕机、设置为维护模式、主从延迟超过设定值都会从路由信息中剔除
7. 如果宕机切换失败，需手动进行强制切换    