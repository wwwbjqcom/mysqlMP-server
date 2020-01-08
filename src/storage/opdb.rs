/*
@author: xiao cai niao
@datetime: 2019/11/6
*/
use actix_web::{web};
use crate::webroute::route::{HostInfo, PostUserInfo, EditInfo, EditMainTain};
use crate::storage::rocks::{DbInfo, KeyValue, CfNameTypeCode, PrefixTypeCode};
use crate::ha::procotol::{DownNodeCheck, RecoveryInfo, ReplicationState, MysqlMonitorStatus};
use std::error::Error;
use crate::ha::nodes_manager::{SlaveInfo};
use serde::{Serialize, Deserialize};
use crate::rand_string;
use crate::ha::procotol::MysqlState;
use crate::ha::sys_manager::MonitorSetting;
use crate::webroute::new_route::ResponseMonitorStatic;
use std::str::from_utf8;


///
/// mysql node info， insert to rocksdb
///
///
pub fn insert_mysql_host_info(data: web::Data<DbInfo>, info: &web::Json<HostInfo>) -> Result<(), Box<dyn Error>> {
    let check_unique = data.get(&info.host, &CfNameTypeCode::HaNodesInfo.get());
    match check_unique {
        Ok(v) => {
            if v.value.len() > 0 {
                let a = format!("this key: ({}) already exists in the database",&info.host);
                return Err(a.into());
            }
        }
        _ => {}
    }
    let v = HostInfoValue::new(info)?;
    v.save(&data)?;

    //初始化节点监控配置
    let monitor_info = MonitorSetting::new(&info.host);
    monitor_info.save(&data)?;
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HaChangeLog {
    pub key: String,                        //格式 host_timestamp  host为宕机节点
    pub cluster_name: String,
    pub old_master_info:  DownNodeCheck,    //宕机节点信息
    pub new_master_binlog_info: SlaveInfo,  //如果宕机节点在切换之前未进行binlog追加将保存新master读取到的binlog信息，在宕机节点恢复时会进行判断回滚
    pub recovery_info: RecoveryInfo,        //宕机恢复同步所需的新master信息
    pub recovery_status: bool,              //是否已恢复
    pub switch_status: bool,                //切换状态
}

impl HaChangeLog {
    pub fn new() -> HaChangeLog {
        HaChangeLog{
            key: "".to_string(),
            cluster_name: "".to_string(),
            old_master_info: DownNodeCheck { host: "".to_string(), dbport: 0 },
            new_master_binlog_info: SlaveInfo {
                host: "".to_string(),
                dbport: 0,
                slave_info: ReplicationState {
                    log_name: "".to_string(),
                    read_log_pos: 0,
                    exec_log_pos: 0
                },
                new_master: false
            },
            recovery_info: RecoveryInfo {
                binlog: "".to_string(),
                position: 0,
                gtid: "".to_string(),
                masterhost: "".to_string(),
                masterport: 0,
                read_binlog: "".to_string(),
                read_position: 0
            },
            recovery_status: false,
            switch_status: false
        }
    }

    pub fn save(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        let key = format!("{}_{}",self.key.clone(), crate::timestamp());
        let value = serde_json::to_string(self)?;
        let row = KeyValue{key, value};
        db.put(&row, &CfNameTypeCode::HaChangeLog.get())?;
        return Ok(());
    }

    pub fn update(&mut self, db: &web::Data<DbInfo>, row_key: String) -> Result<(), Box<dyn Error>> {
        let value = serde_json::to_string(self)?;
        let row = KeyValue{key: row_key, value};
        db.put(&row, &CfNameTypeCode::HaChangeLog.get())?;
        return Ok(());
    }
}

///
///
///
/// 用户信息结构
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserInfo {
    pub user_name: String,
    pub password: String,
    pub hook_id: String,
    pub create_time: i64,
    pub update_time: i64
}

impl UserInfo {
    pub fn new(info: &PostUserInfo) -> UserInfo {
        let create_time = crate::timestamp();
        let update_time = crate::timestamp();
        UserInfo{
            user_name: info.user_name.clone(),
            password: info.password.clone(),
            hook_id: rand_string(),
            create_time,
            update_time
        }
    }
}

///
///
///
/// 节点基础信息, host做为key
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HostInfoValue {
    pub host: String,   //127.0.0.1:3306
    pub dbport: usize,  //default 3306
    pub rtype: String,  //db、route
    pub cluster_name: String,   //集群名称,route类型默认default
    pub online: bool,   //db是否在线， true、false
    pub insert_time: i64,
    pub update_time: i64,
    pub maintain: bool, //是否处于维护模式，true、false
}

impl HostInfoValue {
    pub fn new(info: &HostInfo) -> Result<HostInfoValue, Box<dyn Error>> {
        let h = HostInfoValue{
            host: info.host.clone(),
            rtype: info.rtype.clone(),
            dbport: info.dbport.clone(),
            cluster_name: info.cluster_name.clone(),
            online: false,
            insert_time: crate::timestamp(),
            update_time: crate::timestamp(),
            maintain: false
        };
        Ok(h)
    }

    ///
    /// 写入db
    pub fn save(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        let value = serde_json::to_string(&self)?;
        let row = KeyValue{key: self.host.clone(), value};
        db.put(&row, &CfNameTypeCode::HaNodesInfo.get())?;
        Ok(())
    }

    ///
    /// 编辑节点信息
    pub fn edit(&mut self, info: &web::Json<EditInfo>) {
        self.host = info.host.clone();
        self.dbport = info.dbport.clone();
        self.cluster_name = info.cluster_name.clone();
        self.update_time = crate::timestamp();
    }

    ///
    /// 设置节点维护模式状态
    pub fn maintain(&mut self, info: &web::Json<EditMainTain>) {
        if info.maintain == "true".to_string() {
            self.maintain = false;
        }else {
            self.maintain = true;
        }
        self.update_time = crate::timestamp();
    }

    ///
    /// 获取当前节点在db中保存的状态信息
    pub fn get_state(&self, db: &web::Data<DbInfo>) -> Result<MysqlState, Box<dyn Error>> {
        let kv = db.get(&self.host, &CfNameTypeCode::NodesState.get())?;
        if kv.value.len() > 0 {
            let state: MysqlState = serde_json::from_str(&kv.value)?;
            return Ok(state);
        }else {
            //let err = format!("this host: {} no state data", &self.host);
            //return Err(err.into());
            let state = MysqlState::new();
            return Ok(state);
        }
    }

    pub fn get_role(&self, db: &web::Data<DbInfo>) -> Result<String, Box<dyn Error>> {
        let state = self.get_state(db)?;
        Ok(state.role)
    }
}

///
///
///
///
/// 获取db中现有的cluster列表
#[derive(Serialize, Deserialize, Debug)]
pub struct NodeClusterList{
    pub cluster_name_list: Vec<String>
}

impl NodeClusterList{
    pub fn new() -> NodeClusterList{
        NodeClusterList { cluster_name_list: vec![] }
    }

    pub fn init(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        let result = db.iterator(&CfNameTypeCode::HaNodesInfo.get(), &String::from(""))?;
        for row in &result{
            let value: HostInfoValue = serde_json::from_str(&row.value)?;
            if self.is_exists(&value.cluster_name){continue;}
            self.cluster_name_list.push(value.cluster_name.clone());
        }
        Ok(())
    }
    fn is_exists(&self, cluster_name: &String) -> bool {
        for cl in &self.cluster_name_list {
            if cl == cluster_name{
                return true;
            }
        }
        return false;
    }
}


/// 获取route信息中现有的cluster列表
#[derive(Serialize, Deserialize, Debug)]
pub struct RouteClusterList{
    pub cluster_name_list: Vec<String>
}

impl RouteClusterList{
    pub fn new() -> RouteClusterList {
        RouteClusterList{ cluster_name_list: vec![] }
    }

    pub fn init(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        let route_all = db.get_route_all()?;
        for route in &route_all {
            if !self.is_exists(&route.value.cluster_name){
                self.cluster_name_list.push(route.value.cluster_name.clone());
            }
        }
        Ok(())
    }

    fn is_exists(&self, cluster_name: &String) -> bool {
        for cl in &self.cluster_name_list {
            if cl == cluster_name {
                return true;
            }
        }
        return false;
    }
}

///
///
///
///
/// node节点信息
#[derive(Deserialize, Serialize, Debug)]
pub struct NodeInfo{
    pub cluster_name: String,
    pub host: String,
    pub dbport: usize,
    pub online: bool,   //是否在线， true、false
    pub maintain: bool, //是否处于维护模式，true、false
    pub role: String,   //主从角色
    pub master: String,
    pub sql_thread: bool,
    pub io_thread: bool,
    pub seconds_behind: usize,
    pub read_only: bool,
    pub version: String,
    pub executed_gtid_set: String,
    pub innodb_flush_log_at_trx_commit: usize,
    pub sync_binlog: usize,
    pub server_id: usize,
    pub event_scheduler: String,
    pub sql_error: String
}
impl NodeInfo{
    pub fn new(state: &MysqlState, node: &HostInfoValue) -> NodeInfo {
        NodeInfo{
            cluster_name: node.cluster_name.clone(),
            host: node.host.clone(),
            dbport: node.dbport.clone(),
            online: node.online.clone(),
            maintain: node.maintain.clone(),
            role: state.role.clone(),
            master: state.master.clone(),
            sql_thread: state.sql_thread.clone(),
            io_thread: state.io_thread.clone(),
            seconds_behind: state.seconds_behind.clone(),
            read_only: state.read_only.clone(),
            version: state.version.clone(),
            executed_gtid_set: state.executed_gtid_set.clone(),
            innodb_flush_log_at_trx_commit: state.innodb_flush_log_at_trx_commit.clone(),
            sync_binlog: state.sync_binlog.clone(),
            server_id: state.server_id.clone(),
            event_scheduler: state.event_scheduler.clone(),
            sql_error: state.sql_error.clone()
        }
    }
}

///
///
///
///
/// 每个集群节点信息
#[derive(Deserialize, Serialize, Debug)]
pub struct ClusterNodeInfo{
    pub cluster_name: String,
    pub total: usize,
    pub nodes_info: Vec<NodeInfo>
}

impl ClusterNodeInfo{
    pub fn new(cluster_name: &String) -> ClusterNodeInfo{
        ClusterNodeInfo{
            cluster_name: cluster_name.clone(),
            total: 0,
            nodes_info: vec![]
        }
    }

    pub fn init(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        let result = db.iterator(&CfNameTypeCode::HaNodesInfo.get(), &String::from(""))?;
        for row in &result{
            let node: HostInfoValue = serde_json::from_str(&row.value)?;
            if &node.cluster_name == &self.cluster_name{
                let state = node.get_state(db)?;
                let node_info = NodeInfo::new(&state, &node);
                self.total += 1;
                self.nodes_info.push(node_info);
            }
        }
        Ok(())
    }

    ///
    /// 统计所有节点监控信息， 用于首页展示
    ///
    /// 倒叙迭代获取每个节点最后一条数据， 如果每个节点都已获取最后一条数据就退出迭代
    pub fn static_monitor(&self, db: &web::Data<DbInfo>, rsm: &mut ResponseMonitorStatic) -> Result<(), Box<dyn Error>> {
        let cf_name = CfNameTypeCode::SystemData.get();
        let mut tmp: Vec<String> = vec![];
        if let Some(cf) = db.db.cf_handle(&cf_name){
            let mut iter = db.db.raw_iterator_cf(cf)?;
            iter.seek_to_last();
            iter.prev();
            'all: while iter.valid() {
                if tmp.len() == self.nodes_info.len(){
                    break 'all;
                }
                if let Some(s) = iter.key(){
                    let key: String = from_utf8(&s.to_vec())?.parse()?;
                    if key.starts_with(&PrefixTypeCode::NodeMonitorData.prefix()){
                        'b: for n in &self.nodes_info{
                            if key.contains(n.host.as_str()){
                                for t in &tmp{
                                    if t == &n.host{
                                        break 'b;
                                    }
                                }
                                if !self.check_monitor_setting(db, &n.host){
                                    tmp.push(n.host.clone());
                                    continue 'all;
                                }
                                if let Some(v) = iter.value(){
                                    let v: MysqlMonitorStatus = serde_json::from_slice(&v)?;
                                    rsm.update(&v);
                                    tmp.push(n.host.clone());
                                }
                                break 'b;
                            }
                        }
                    }
                }
                //
                //
                iter.prev();
            }
        }
        Ok(())
    }

    fn check_monitor_setting(&self, db: &web::Data<DbInfo>, host: &String) -> bool{
        let a = db.prefix_get(&PrefixTypeCode::NodeMonitorSeting, host);
        match a {
            Ok(v) => {
                if v.value.len() > 0{
                    let value: MonitorSetting = serde_json::from_str(&v.value)?;
                    return value.monitor.clone();
                }
            }
            Err(e) => {
                info!("{}", e.to_string());
            }
        }
        return false;
    }
}

///
///
impl MysqlState{
    pub fn save(&self, db: &web::Data<DbInfo>, key: &String) -> Result<(), Box<dyn Error>> {
        let value = serde_json::to_string(&self)?;
        let a = KeyValue{key: key.clone(), value };
        db.put(&a, &CfNameTypeCode::NodesState.get())?;
        Ok(())
    }
}


///
///
/// slave behind 配置结构体
#[derive(Serialize, Deserialize, Debug)]
pub struct SlaveBehindSetting{
    pub cluster_name: String,
    pub delay: usize
}

impl SlaveBehindSetting{
    pub fn new(cluster_name: &String) -> SlaveBehindSetting {
        SlaveBehindSetting{ cluster_name: cluster_name.clone(), delay: 100 }
    }
    pub fn save(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        db.prefix_put(&PrefixTypeCode::SlaveDelaySeting, &self.cluster_name, &self)?;
        Ok(())
    }
}




