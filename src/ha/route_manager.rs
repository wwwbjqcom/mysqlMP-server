/*
@author: xiao cai niao
@datetime: 2019/12/04
*/
use actix_web::web;
use crate::storage::rocks::{DbInfo, CfNameTypeCode, KeyValue, PrefixTypeCode};
use std::{time, thread};
use crate::ha::procotol::{MysqlState};
use std::error::Error;
use crate::ha::nodes_manager::CheckState;
use crate::storage::opdb::{HaChangeLog, HostInfoValue};
use serde::{Serialize, Deserialize};

///
/// 每个mysql实例ip及端口信息
#[derive(Serialize, Deserialize, Debug)]
pub struct MysqlHostInfo {
    pub host: String,
    pub port: usize
}

///
/// 集群路由信息
///
#[derive(Serialize, Deserialize, Debug)]
pub struct RouteInfo {
    pub cluster_name: String,
    pub write: MysqlHostInfo,
    pub read: Vec<MysqlHostInfo>
}
impl RouteInfo {
    pub fn new(cluster_name: String) -> RouteInfo {
        RouteInfo{
            cluster_name,
            write: MysqlHostInfo { host: "".to_string(), port: 0 },
            read: vec![]
        }
    }

    fn split_str(&self, host_info: String) -> String {
        let host_info = host_info.split(":");
        let host_vec = host_info.collect::<Vec<&str>>();
        return host_vec[0].to_string();
    }

    fn set_master_info(&mut self, node: &NodeInfo) {
        let host = self.split_str(node.value.host.clone());
        self.write = MysqlHostInfo{ host, port: node.value.dbport.clone() };
    }

    fn set_slave_info(&mut self, node: &NodeInfo) {
        let host = self.split_str(node.value.host.clone());
        self.read.push(MysqlHostInfo{host, port: node.value.dbport.clone()});
    }

    ///
    /// 获取宕机状态数据， 检查是db还是client宕机
    ///
    /// 如果检查的为master:
    /// mysql实例宕机继续检查recovery状态， 检查switch状态是否成功，如果成功返回false剔除该节点，使用新master
    /// 如果switch状态为false或者db中没有recovery数据, 将不对该cluster路由信息做修改，返回error
    ///
    /// 原因有二： 有可能切换失败， 有可能正在切换中
    ///
    /// client宕机将直接返回true
    fn check_down_status(&mut self, key: &String, db: &web::Data<DbInfo>, role: String) -> Result<bool, Box<dyn Error>> {
        let result = db.get(key, &CfNameTypeCode::CheckState.get())?;
        //info!("check_status: {}:{:?}", key, result);
        let value: CheckState = serde_json::from_str(&result.value)?;
        if value.db_down {
            if role == "master".to_string() {
                self.check_recovery_status(key, db)?;
                return Ok(false)
            }else if role == "slave".to_string() {
                return Ok(false)
            }
        }
        return Ok(true);
    }

    fn check_recovery_status(&self, key: &String, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        let result = db.prefix_iterator(key, &CfNameTypeCode::HaChangeLog.get())?;
        //info!("{:?}", result);
        let mut tmp = vec![];
        for row in result {
            if row.key.starts_with(key){
                tmp.push(row);
            }
        }

        if tmp.len() > 0 {
            tmp.sort_by(|a, b| b.key.cmp(&a.key));
            let key = tmp[0].key.clone();
            let value: HaChangeLog = serde_json::from_str(&tmp[0].value)?;
            //info!("{:?}", value);
            //判断切换状态， 如果为成功则需再次判断是否已恢复，如果是已恢复状态表示是旧数据
            //因为正常切换恢复和切换之间至少得有时间差， 有可能在进行路由判断时正处在切换的时候
            //这个时候没有切换数据，这里就会获取到最后一条
            if value.switch_status{
                if !value.recovery_status{
                    //这里继续执行表示最后一条数据未正常恢复
                    //比较时间， 和当前时间进行比较如果超过10秒表示有可能是脏数据，将不进行操作
                    self.check_time_dif(&key)?;
                    return Ok(());
                }
            }

            //如果表示最后一条所有状态都正常或者未正常切换，比较时间差，超过10秒表示为脏数据不进行操作
            self.check_time_dif(&key)?;
            //
        }
        //到这里如果还没返回，表示有可能还在切换中返回一个错误不进行操作
        let err = format!("host: {} is master, but status unusual", key);
        return Err(err.into());
    }

    fn check_time_dif(&self, key: &String) -> Result<(), Box<dyn Error>>{
        let tmp_list = key.split("_");
        let tmp_list = tmp_list.collect::<Vec<&str>>();
        let tmp_time: i64 = tmp_list[1].to_string().parse()?;
        if (crate::timestamp() - tmp_time) > 10000 as i64 {
            let err = format!("key: {} recovery status unusual", key);
            return Err(err.into());
        }
        Ok(())
    }

}
///
/// 节点信息
#[derive(Clone, Debug)]
struct NodeInfo {
    key: String,
    value: HostInfoValue
}
impl NodeInfo {
    fn new(kv: &KeyValue) -> Result<NodeInfo, Box<dyn Error>> {
        let key = kv.key.clone();
        let value = serde_json::from_str(&kv.value)?;
        Ok(NodeInfo{
            key,
            value
        })
    }
}

///
/// 集群信息
#[derive(Debug, Clone)]
struct ClusterNodeInfo{
    cluster_name: String,
    slave_behind_setting: usize,    //slave 延迟配置
    node_list: Vec<NodeInfo>
}
impl ClusterNodeInfo {
    fn new(ninfo: &NodeInfo, slave_behind: usize) -> ClusterNodeInfo {
        ClusterNodeInfo{
            cluster_name: ninfo.value.cluster_name.clone(),
            slave_behind_setting: slave_behind,
            node_list: vec![ninfo.clone()]
        }
    }

    fn my_clone(&self) -> ClusterNodeInfo {
        ClusterNodeInfo{
            cluster_name: self.cluster_name.clone(),
            slave_behind_setting: self.slave_behind_setting.clone(),
            node_list: self.node_list.clone(),
        }
    }

    fn update(&mut self, ninfo: &NodeInfo) {
        self.node_list.push(ninfo.clone());
    }

    fn route_check(&self, db: &web::Data<DbInfo>) -> Result<RouteInfo, Box<dyn Error>> {
        let mut route_info = RouteInfo::new(self.cluster_name.clone());
        for node in &self.node_list{
            let cur_state = node.value.get_state(db)?;
            if self.master_check(&node, &cur_state, db, &mut route_info)?{
                continue;
            };
            self.slave_check(&node, &cur_state, db, &mut route_info)?;
        }
        Ok(route_info)
    }

    ///
    /// 对role为master的节点进行判断， 如果为online直接写入信息，如果宕机则需要检查宕机检查数据是否为实例宕机，如果为实例宕机则需要检查是否已经切换
    /// 因为在实例或者client宕机时则不会更新检查状态，所以宕机之前为master如果未恢复则会一直为master状态
    fn master_check(&self, node: &NodeInfo, node_status: &MysqlState, db: &web::Data<DbInfo>, route_info: &mut RouteInfo) -> Result<bool, Box<dyn Error>> {
        //info!("{:?}", node_status);
        if node_status.role == "master".to_string() {
            //info!("master_check: {:?}",node_status);
            if node.value.online {
                route_info.set_master_info(node);
                return Ok(true);
            }else {
                if route_info.check_down_status(&node.key, db, "master".to_string())?{
                    route_info.set_master_info(node);
                    return Ok(true);
                };
            }
        }
        Ok(false)
    }

    ///
    /// 检查slave节点状态
    /// 1、在线时需要检查replication延迟状态， behind超过100将剔除该节点，如果配置为0表示不检查延迟
    /// 2、如果宕机则需要检查是实例宕机还是client宕机
    /// 3、如果为实例宕机直接剔除
    /// 4、如果client宕机将不做任何操作， 直接添加对应节点， 这里无法检测hebind值，因为如果client宕机将不会更新状态
    fn slave_check(&self, node: &NodeInfo, node_status: &MysqlState, db: &web::Data<DbInfo>, route_info: &mut RouteInfo) -> Result<(), Box<dyn Error>> {
        //info!("{:?}", node_status);
        if node.value.maintain{return Ok(())}
        if node_status.role == "slave".to_string() {
            if node.value.online{
                if !node_status.sql_thread {
                    return Ok(());
                }
                if !node_status.io_thread {
                    return Ok(())
                }
                if self.slave_behind_setting == 0{
                    //为0表示不判断延迟
                    route_info.set_slave_info(node);
                }
                else if node_status.seconds_behind <= self.slave_behind_setting {
                    route_info.set_slave_info(node);
                }
            }else {
                if route_info.check_down_status(&node.key, db, "slave".to_string())?{
                    route_info.set_slave_info(node);
                }
            }
        }
        Ok(())
    }

}

///
/// 所有节点信息
#[derive(Debug)]
struct AllNode {
    nodes: Vec<ClusterNodeInfo>
}
impl AllNode {
    ///
    /// 从db获取所有节点并通过cluster_name进行分类
    fn new(db: &web::Data<DbInfo>) -> Result<AllNode, Box<dyn Error>> {
        let all_node = db.iterator(&CfNameTypeCode::HaNodesInfo.get(), &"".to_string())?;
        let mut nodes_info: Vec<ClusterNodeInfo> = vec![];
        'all: for node in all_node {

            let ninfo = NodeInfo::new(&node)?;
            if ninfo.value.rtype == "route".to_string() {
                continue 'all;
            }
            'inner: for (idx,cluster_info) in nodes_info.iter().enumerate(){
                let mut my_cluster_info = cluster_info.my_clone();
                if &my_cluster_info.cluster_name == &ninfo.value.cluster_name {
                    my_cluster_info.update(&ninfo);
                    nodes_info[idx] = my_cluster_info;
                    continue 'all;
                }
            }
            let mut delay = 100;
            let delay_check = db.get_hehind_setting(&ninfo.value.cluster_name);
            match delay_check {
                Ok(v) => {
                    delay = v.delay;
                }
                Err(e) => {
                    info!("check slave behind setting for cluster_name:{} , Error: {:?}", &ninfo.value.cluster_name,e.to_string());
                }
            }
            nodes_info.push(ClusterNodeInfo::new(&ninfo, delay));
        }
        Ok(AllNode{
            nodes: nodes_info
        })
    }


    ///
    /// 对cluster信息进行循环检查，并把对应route信息写入db
    fn route_manager(&self, db: &web::Data<DbInfo>) {
        for cluster in &self.nodes {
            self.run_check_state(cluster,db);
        }
    }

    fn run_check_state(&self, cluster: &ClusterNodeInfo, db: &web::Data<DbInfo>){
        let check_state = cluster.route_check(db);
        // info!("{:?}", check_state);
        match check_state {
            Ok(rinfo) => {
                if rinfo.write.host == "".to_string(){
                    thread::sleep(time::Duration::from_secs(1));
                    self.run_check_state(cluster, db);
                    return;
                }
                if let Err(e) = db.prefix_put(&PrefixTypeCode::RouteInfo, &rinfo.cluster_name, &rinfo){
                    info!("{:?}", e.to_string());
                };
            }
            Err(_e) => {
                //info!("Error: {:?} for cluster: {:?}", e.to_string(), &cluster);
            }
        }

    }
}


pub fn manager(db: web::Data<DbInfo>) {
    let mut all_node = AllNode::new(&db).unwrap();
    let mut start_time = crate::timestamp();
    loop {
        if crate::timestamp() - start_time >= 10000 {
            //每10秒获取一次rocksdb中节点信息
            all_node = AllNode::new(&db).unwrap();
            //info!("node list: {:?}",all_node);
            start_time = crate::timestamp();
        }
        all_node.route_manager(&db);
        thread::sleep(time::Duration::from_secs(1));
    }
}