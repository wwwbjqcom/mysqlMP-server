/*
@author: xiao cai niao
@datetime: 2019/11/22
*/

use actix_web::{web};
use std::sync::{mpsc, Arc, Mutex};
use crate::storage::rocks::{DbInfo, KeyValue, CfNameTypeCode};
use crate::ha::{DownNodeInfo, get_node_state_from_host};
use crate::ha::procotol;
use std::error::Error;
use crate::ha::procotol::{HostInfoValue, DownNodeCheckStatus, MyProtocol, ReplicationState, DownNodeCheck, MysqlState, ChangeMasterInfo, RecoveryInfo, HostInfoValueGetAllState, BinlogValue, SyncBinlogInfo};
use std::thread;
use std::time::Duration;
use serde::{Serialize, Deserialize};
use crate::storage::opdb::HaChangeLog;

///
///用于检查状态
///
#[derive(Serialize, Deserialize, Debug)]
pub struct CheckState {
    pub db_offline: usize,
    pub client_offline: usize,
    pub all_nodes: usize,
    pub db_down: bool,
    pub client_down: bool,
    pub role: String,
}
impl CheckState {
    fn new(all_nodes: usize) -> CheckState{
        CheckState{ db_offline: 0, client_offline: 0, all_nodes, db_down: false, client_down: false, role: "".to_string() }
    }
    fn check(&mut self, state: &DownNodeCheckStatus) {
        if state.host.len() > 0 {
            if !state.client_status{
                self.client_offline += 1;
            }
            if !state.db_status {
                self.db_offline += 1;
            }

            if self.db_offline >= (self.all_nodes/2+1) {
                self.db_down = true;
            }
            if self.client_offline >= (self.all_nodes/2+1) {
                self.client_down = true;
            }
        }
    }

    fn update_db(&self, db: &web::Data<DbInfo>, key: &String) -> Result<(), Box<dyn Error>> {
        let value = serde_json::to_string(&self)?;
        let a = KeyValue{key: key.clone(), value: (&value).parse()? };
        db.put(&a, &CfNameTypeCode::CheckState.get())?;
        Ok(())
    }

    fn delete_from_db(&self, db: &web::Data<DbInfo>, key: &String) {
        if let Err(e) = db.delete(key, &CfNameTypeCode::CheckState.get()){
            info!("{:?}",e.to_string());
        };
    }

    fn is_slave(&self, db: &web::Data<DbInfo>, key: &String) -> Result<bool, Box<dyn Error>> {
        let result = db.get(key, &CfNameTypeCode::CheckState.get())?;
        let value: CheckState = serde_json::from_str(&result.value)?;
        info!("{:?}", &value);
        if value.role == "master".to_string(){
            return Ok(false);
        }
        return Ok(true);
    }

    fn is_client_down(&self, db: &web::Data<DbInfo>, key: &String) -> Result<bool, Box<dyn Error>> {
        let result = db.get(key, &CfNameTypeCode::CheckState.get())?;
        let value: CheckState = serde_json::from_str(&result.value)?;
        if value.db_down {
            return Ok(false);
        }
        return Ok(true);
    }
}

///
///主要负责master宕机时新节点选举及切换、追加日志操作
///
pub fn manager(db: web::Data<DbInfo>,  rec: mpsc::Receiver<DownNodeInfo>){
    loop {
        let r = rec.recv().unwrap();
        if !r.online {
            info!("host {:?} is down for cluster {:?}....", r.host, r.cluster_name);
            info!("check network......");
            //let nodes = crate::ha::get_nodes_info(&db);
            let down_node = procotol::DownNodeCheck::new(r.host, r.dbport);
            let mut elc = ElectionMaster::new(r.cluster_name.clone(), down_node);
            if let Err(e) = elc.election(&db){
                info!("{}", e.to_string());
            };
        }else {
            info!("host: {} is running...", &r.host);
            let state = CheckState::new(0);

            if let Ok(f) = state.is_slave(&db, &r.host){
                if f{
                    info!("slave node: {}, delete status now...", &r.host);
                    state.delete_from_db(&db, &r.host);
                    info!("Ok");
                    continue;
                }
            }

            if let Ok(f) = state.is_client_down(&db, &r.host) {
                if f {
                    info!("node: {} client, delete status now...", &r.host);
                    state.delete_from_db(&db, &r.host);
                    info!("Ok");
                    continue;
                }
            }

            info!("start recovery...");
            let mut reco = RecoveryDownNode::new(r.host.clone());
            if let Err(e) = reco.recovery(&db){
                info!("Error: {}", e.to_string());
                continue;
            }
            info!("node: {} recovery success, delete status now...", r.host);
            state.delete_from_db(&db, &r.host);
            info!("Ok");
        }
    }
}
///
/// 宕机节点恢复
///
pub struct RecoveryDownNode{
    host: String,
    ha_log: HaChangeLog,
    ha_log_key: String,
}

impl RecoveryDownNode {
    fn new(host: String) -> RecoveryDownNode {
        RecoveryDownNode{ host,
            ha_log: HaChangeLog::new(),
            ha_log_key: "".to_string(),
        }
    }

    fn recovery(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        self.get_recovery_info(db)?;
        if !self.ha_log.switch_status{
            info!("when the machine({}) was shut down during the year, the switchover failed, and the recovery operation could not be performed",&self.host);
            return Ok(());
        }

        if !self.ha_log.recovery_status {
            info!("recovery info: {:?}", self.ha_log.recovery_info);
            let row_sql = MyProtocol::RecoveryCluster.recovery(&self.host, &self.ha_log.recovery_info)?;
            info!("{:?}", row_sql);
            self.update_state(db)?;
            //宕机恢复完成
            return Ok(())
        }

        Ok(())
    }

    fn get_recovery_info(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        let result = db.prefix_iterator(&self.host, &CfNameTypeCode::HaChangeLog.get())?;
        let mut tmp = vec![];
        for row in result {
            if row.key.starts_with(&self.host){
                tmp.push(row);
            }
        }
        if tmp.len() > 0 {
            tmp.sort_by(|a, b| b.key.cmp(&a.key));
            let value: HaChangeLog = serde_json::from_str(&tmp[0].value)?;
            self.ha_log_key = tmp[0].key.clone();
            self.ha_log = value;
        }
        Ok(())
    }

    fn update_state(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        self.ha_log.recovery_status = true;
        self.ha_log.update(db, self.ha_log_key.clone())?;
        Ok(())
    }
}



///
/// 用于切换主从关系、保存切换日志等信息
///
pub struct ElectionMaster {
    pub cluster_name: String,
    pub down_node_info: DownNodeCheck,
    pub check_state: CheckState,            //复检情况
    pub slave_nodes: Vec<SlaveInfo>,
    pub ha_log: HaChangeLog,                //切换日志
    //recovery_info: RecoveryInfo,
}

impl ElectionMaster {
    fn new(cluster_name: String, down_node_info: DownNodeCheck) -> ElectionMaster {
        let mut el = ElectionMaster{
            cluster_name: cluster_name.clone(),
            down_node_info: down_node_info.clone(),
            check_state: CheckState {
                db_offline: 0,
                client_offline: 0,
                all_nodes: 0,
                db_down: false,
                client_down: false,
                role: "".to_string()
            },
            slave_nodes: vec![],
            ha_log: HaChangeLog::new()
        };
        el.ha_log.old_master_info = down_node_info.clone();
        el.ha_log.key = down_node_info.host.clone();
        el.ha_log.cluster_name = cluster_name.clone();
        return el;
    }

    fn election(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        let result = db.iterator(&CfNameTypeCode::HaNodesInfo.get(),&String::from(""))?;
        let (rt, rc) = mpsc::channel();
        let rt= Arc::new(Mutex::new(rt));
        let mut count = 0 as usize;
        for nodes in result {
            if nodes.key != self.down_node_info.host {
                let state: HostInfoValue = serde_json::from_str(&nodes.value)?;
                if !state.online {
                    continue;
                }

                if state.rtype == "route"{
                    continue;
                }

                if state.cluster_name == self.cluster_name {
                    let s = SlaveInfo::new(nodes.key.clone(), state.dbport.clone(), db)?;
                    self.slave_nodes.push(s);
                }
                count += 1;
                let my_rt = Arc::clone(&rt);
                let my_down_node = self.down_node_info.clone();
                thread::spawn(move||{
                    get_down_state_from_node(&state.host, &my_down_node, my_rt);
                });
            }
        }

        self.check_state = CheckState::new(count);
        for _i in 0..count {
            let state = rc.recv_timeout(Duration::new(2,5));
            match state {
                Ok(s) => {
                    self.check_state.check(&s);
                }
                Err(e) => {
                    info!("host {} check error: {:}", &self.down_node_info.host, e.to_string());
                    self.check_state.all_nodes -= 1;
                    continue;
                }
            }
        }
        self.change(db)?;
        self.ha_log.recovery_status = false;
        self.ha_log.switch_status = true;
        self.ha_log.save(db)?;
        Ok(())
    }

    ///
    /// 执行切换操作
    /// 
    fn change(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        if !self.is_master(db)?{
            info!("host: {} is slave, exece change route info...",&self.down_node_info.host);
            return Ok(());
        }
        self.check_state.update_db(&db, &self.down_node_info.host)?;
        info!("{:?}", self.check_state);
        if self.check_state.db_down {
            // mysql实例宕机
            let change_master_info = self.elc_new_master()?;
            info!("election master info : {:?}",change_master_info);
            info!("{:?}", self.slave_nodes);
            if self.check_state.client_down {
                //直接切换
                self.execute_switch_master(db, &change_master_info)?;
            }else {
                //client在线、判断是否有需要追加的数据
                let binlog_value = self.pull_downnode_binlog()?;
                self.push_downnode_binlog_to(&binlog_value)?;
                self.reacquire_recovery_info()?;
                self.execute_switch_master(db, &change_master_info)?;
            }
        }
        Ok(())
    }

    fn push_downnode_binlog_to(&self, buf: &BinlogValue) -> Result<(), Box<dyn Error>> {
        if buf.value.len() > 0 {
            info!("append binlog....");
            let rowsql = MyProtocol::PushBinlog.push_binlog(&self.ha_log.new_master_binlog_info.host, buf)?;
            info!("{:?}", rowsql);
            //执行数据保存
            return Ok(())
        }
        info!("no binlog content");
        return Ok(());
    }

    fn pull_downnode_binlog(&self) -> Result<BinlogValue, Box<dyn Error>> {
        info!("pull difference binlog from {}", &self.down_node_info.host);
        let mut binlog = BinlogValue{ value: vec![] };
        let sync_info = SyncBinlogInfo{
            binlog: self.ha_log.new_master_binlog_info.slave_info.log_name.clone(),
            position: self.ha_log.new_master_binlog_info.slave_info.read_log_pos.clone()
        };
        if sync_info.binlog.len() > 0 {
            info!("pull info: {:?}", &sync_info);
            binlog = MyProtocol::PullBinlog.pull_binlog(&self.down_node_info.host, &sync_info)?;
        }
        return Ok(binlog);
    }

    ///
    /// 获取recovery_info，用于追加binlog之后
    fn reacquire_recovery_info(&mut self) -> Result<(), Box<dyn Error>> {
        for node in &self.slave_nodes{
            if node.new_master{
                self.ha_log.recovery_info = RecoveryInfo::new(node)?;
                self.ha_log.recovery_info.read_binlog = "".to_string();
                self.ha_log.recovery_info.read_position = 0;
            }
        }
        Ok(())
    }

    fn execute_switch_master(&mut self, db: &web::Data<DbInfo>, change_info: &ChangeMasterInfo) -> Result<(), Box<dyn Error>> {
        for slave in &self.slave_nodes{
            if slave.new_master {
                info!("send to new master:{}....",&slave.host);
                if let Err(e) = MyProtocol::SetMaster.send_myself(&slave.host){
                    self.ha_log.save(db)?;
                    return Err(e);
                };
                info!("OK");
            }else {
                info!("send change master info to slave node: {}....", &slave.host);
                if let Err(e) = MyProtocol::ChangeMaster.change_master(&slave.host, change_info){
                    self.ha_log.save(db)?;
                    return Err(e);
                };
                info!("OK");
            }
        }
        Ok(())
    }

    ///
    /// 通过read_binlog信息选举新master
    /// 
    fn elc_new_master(&mut self) -> Result<ChangeMasterInfo, Box<dyn Error>>{
        info!("election new master node.....");
        let mut index: usize = 0;
        let mut read_binlog_pos: usize = 0;
        for (idx,slave_node) in self.slave_nodes.iter().enumerate() {
            index = idx;
            if read_binlog_pos < slave_node.slave_info.read_log_pos {
                read_binlog_pos = slave_node.slave_info.read_log_pos.clone();
            }
        }
        self.slave_nodes[index].new_master = true;
        info!("new master host: {}", &self.slave_nodes[index].host);
        let dbport = self.slave_nodes[index].dbport.clone();
        let host_info = self.slave_nodes[index].host.clone();
        let host_info = host_info.split(":");
        let host_vec = host_info.collect::<Vec<&str>>();
        info!("get recovery info from {}", &self.slave_nodes[index].host);
        self.ha_log.recovery_info = RecoveryInfo::new(&self.slave_nodes[index])?;
        self.ha_log.new_master_binlog_info = self.slave_nodes[index].clone();
        info!("Ok");
        let cm = ChangeMasterInfo{ master_host: host_vec[0].to_string(), master_port: dbport};
        return Ok(cm);
    }

    fn is_master(&mut self, db: &web::Data<DbInfo>) -> Result<bool, Box<dyn Error>> {
        if let Ok(r) = db.get(&self.down_node_info.host, &CfNameTypeCode::NodesState.get()){
            let state: MysqlState = serde_json::from_str(&r.value)?;
            if state.role == "master".to_string() {
                self.check_state.role = "master".to_string();
                return Ok(true);
            }
        };
        self.check_state.role = "slave".to_string();
        return Ok(false);
    }
}

///
/// 分发每个node检查宕机节点状态
///
fn get_down_state_from_node(host_info: &String,
                            down_node: &procotol::DownNodeCheck,
                            sender: Arc<Mutex<mpsc::Sender<DownNodeCheckStatus>>>) {
    if let Ok(value) = MyProtocol::DownNodeCheck.down_node_check(host_info, down_node){
        info!("{}: {:?}",host_info,value);
        sender.lock().unwrap().send(value).unwrap();
    };
}

///
/// 判断集群内slave节点状态
/// 根据读取binlog位置情况选举新master
/// 并对其余节点执行changemaster
///
#[derive(Serialize, Clone, Debug, Deserialize)]
pub struct SlaveInfo {
    pub host: String,
    pub dbport: usize,
    pub slave_info: ReplicationState,
    pub new_master: bool,
}
impl SlaveInfo {
    fn new(host: String, dbport: usize, db: &web::Data<DbInfo>) -> Result<SlaveInfo, Box<dyn Error>> {
        let node_info = db.get(&host, &CfNameTypeCode::NodesState.get())?;
        let node_info: MysqlState = serde_json::from_str(&node_info.value)?;
        Ok(SlaveInfo {
            host,
            dbport,
            slave_info: ReplicationState {
                log_name: node_info.master_log_file,
                read_log_pos: node_info.read_master_log_pos,
                exec_log_pos: node_info.exec_master_log_pos
            },
            new_master: false
        })
    }
}

///
/// 主动切换
pub struct SwitchForNodes {
    pub host: String,
    pub dbport: usize,
    pub cluster_name: String,
    pub old_master_info: HostInfoValueGetAllState,
    pub slave_nodes_info: Vec<HostInfoValueGetAllState>,
    pub repl_info: ChangeMasterInfo,
    pub success_slave_host: Vec<String>,
}

impl SwitchForNodes {
    pub fn new(host: &String) -> SwitchForNodes {
        SwitchForNodes{
            host: host.clone(),
            dbport: 0,
            cluster_name: "".to_string(),
            old_master_info: HostInfoValueGetAllState {
                host: "".to_string(),
                dbport: 0,
                rtype: "".to_string(),
                online: false,
                maintain: false,
                role: "".to_string(),
                cluster_name: "".to_string()
            },

            slave_nodes_info: vec![],
            repl_info: ChangeMasterInfo{ master_host: "".to_string(), master_port: 0 },
            success_slave_host: vec![]
        }
    }

    pub fn switch(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        info!("start.....");
        let cf_name = CfNameTypeCode::HaNodesInfo.get();
        let node_info = db.get(&self.host, &cf_name)?;
        let node_info: HostInfoValue = serde_json::from_str(&node_info.value)?;
        self.cluster_name = node_info.cluster_name;
        self.dbport = node_info.dbport;
        self.get_all_nodes_for_cluster_name(&db, &cf_name)?;
        self.set_master_variables()?;
        self.get_repl_info()?;
        if let Err(e) = self.run_switch(){
            info!("switch error: {}", &e.to_string());
            self.rollback_switch()?;
            return Err(e);
        };
        info!("Ok");
        Ok(())
    }

    fn get_all_nodes_for_cluster_name(&mut self, db: &web::Data<DbInfo>, cf_name: &String) -> Result<(), Box<dyn Error>> {
        let result = db.iterator(cf_name,&String::from(""))?;
        for row in result {
            let value: HostInfoValue = serde_json::from_str(&row.value)?;
            if value.maintain{continue;};
            if value.host == self.host {
                let role = crate::webroute::route::get_nodes_role(db, &row.key);
                if role == String::from("master"){
                    let a = String::from("do not allow the current master to perform this operation");
                    return  Err(a.into());
                }
                continue;
            }
            if value.cluster_name == self.cluster_name {
                let role = crate::webroute::route::get_nodes_role(db, &row.key);
                let v = crate::ha::procotol::HostInfoValueGetAllState::new(&value, role.clone());
                if role == String::from("master"){
                    self.old_master_info = v;
                }else {
                    self.slave_nodes_info.push(v);
                }
            }
        }
        Ok(())
    }

    ///
    /// 对当前master进行read only设置，关闭写入直到slave延迟为0
    ///
    fn set_master_variables(&self) -> Result<(), Box<dyn Error>> {
        info!("set old master is readonly...");
        MyProtocol::SetVariables.send_myself(&self.old_master_info.host)?;
        Ok(())
    }

    ///
    /// 从新master获取gtid等信息，用于其他slave指向操作
    ///
    /// 因为有可能提升为master的节点上gtid信息和其他的不一致
    ///
    /// 需等待seconds_behind为0时才进行切换
    ///
    fn get_repl_info(&mut self) -> Result<(), Box<dyn Error>> {
        info!("wait new master seconds_behind is zero");
        loop {
            let state = get_node_state_from_host(&self.host)?;
            if state.seconds_behind > 0 { continue; };
            info!("get repl info from new master...");
            //self.repl_info = RecoveryInfo::new(self.host.clone(), self.dbport.clone())?;
            self.repl_info = ChangeMasterInfo::new(self.host.clone(), self.dbport.clone());
            info!("replication info: {:?}", &self.repl_info);
            break;
        }
        Ok(())
    }

    fn run_switch(&mut self) -> Result<(), Box<dyn Error>> {
        self.switch_slave()?;
        //切换旧master为slave
        info!("change old master {}", &self.old_master_info.host);
        MyProtocol::ChangeMaster.change_master(&self.old_master_info.host, &self.repl_info)?;
        info!("set master for {}", &self.host);
        MyProtocol::SetMaster.send_myself(&self.host)?;
        return Ok(());

    }

    fn switch_slave(&mut self) -> Result<(), Box<dyn Error>> {
        for slave in &self.slave_nodes_info {
            info!("change {}", &slave.host);
            MyProtocol::ChangeMaster.change_master(&slave.host, &self.repl_info)?;
            self.success_slave_host.push(slave.host.clone());
        }
        Ok(())
    }

    fn rollback_switch(&mut self) -> Result<(), Box<dyn Error>> {
        info!("rollback switch...");
        info!("rollback old master...");
        MyProtocol::SetMaster.send_myself(&self.old_master_info.host)?;
        self.repl_info = ChangeMasterInfo::new(self.old_master_info.host.clone(), self.old_master_info.dbport.clone());
        self.switch_slave()?;
        info!("rollback back new master...");
        MyProtocol::ChangeMaster.change_master(&self.host, &self.repl_info)?;
        return Ok(());
    }
}





