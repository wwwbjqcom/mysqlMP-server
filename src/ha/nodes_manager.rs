/*
@author: xiao cai niao
@datetime: 2019/11/22
*/

use actix_web::web;
use std::sync::{mpsc, Arc, Mutex};
use crate::storage::rocks::{DbInfo, KeyValue};
use crate::ha::{DownNodeInfo, conn, get_node_state_from_host};
use crate::ha::procotol;
use std::error::Error;
use crate::ha::procotol::{HostInfoValue, DownNodeCheckStatus, rec_packet, MyProtocol, ReplicationState, DownNodeCheck, MysqlState, ChangeMasterInfo, RecoveryInfo, send_value_packet, HostInfoValueGetAllState};
use std::thread;
use std::time::Duration;
use serde::{Serialize, Deserialize};
use crate::storage::opdb::HaChangeLog;
use std::net::TcpStream;

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
}
impl CheckState {
    fn new(all_nodes: usize) -> CheckState{
        CheckState{ db_offline: 0, client_offline: 0, all_nodes, db_down: false, client_down: false}
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
        let cf_name = String::from("Check_state");
        let value = serde_json::to_string(&self)?;
        let a = KeyValue{key: key.clone(), value: (&value).parse()? };
        db.put(&a, &cf_name)?;
        Ok(())
    }

    fn delete_from_db(&self, db: &web::Data<DbInfo>, key: &String) -> Result<(), Box<dyn Error>> {
        let cf_name = String::from("Check_state");
        db.delete(key, &cf_name)?;
        Ok(())
    }
}

///
///主要负责master宕机时新节点选举及切换、追加日志操作
///
pub fn manager(db: web::Data<DbInfo>,  rec: mpsc::Receiver<DownNodeInfo>){
    loop {
        let r = rec.recv().unwrap();
        if !r.online {
            info!("master {:?} is down for cluster {:?}....", r.host, r.cluster_name);
            info!("check network......");
            //let nodes = crate::ha::get_nodes_info(&db);
            let down_node = procotol::DownNodeCheck::new(r.host, r.dbport);
            let mut elc = ElectionMaster::new(r.cluster_name.clone(), down_node);
            let _state = elc.election(&db);
        }else {
            info!("node: {} recovery success, delete status now...", r.host);
            let state = CheckState::new(0);
            if let Err(e) = state.delete_from_db(&db, &r.host){
                info!("{:?}",e.to_string());
            };
            info!("Ok");
        }
    }
}


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
        ElectionMaster{
            cluster_name: cluster_name.clone(),
            down_node_info: down_node_info.clone(),
            check_state: CheckState {
                db_offline: 0,
                client_offline: 0,
                all_nodes: 0,
                db_down: false,
                client_down: false
            },
            slave_nodes: vec![],
            ha_log: HaChangeLog::new()
        }
    }

    fn election(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        let cf_name = String::from("Ha_nodes_info");
        let result = db.iterator(&cf_name,&String::from(""))?;
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
        self.check_state.update_db(&db, &self.down_node_info.host)?;
        info!("{:?}", self.check_state);
        if self.check_state.db_down {
            // mysql实例宕机
            //if self.check_state.client_down {
                //直接切换
                let change_master_info = self.elc_new_master();
                info!("election master info : {:?}",change_master_info);
                for slave in &self.slave_nodes{
                    if slave.new_master {
                        info!("send to new master:{}....",&slave.host);
                        self.ha_log.new_master_binlog_info = slave.clone();
                        if let Err(e) = self.exec_change(&slave.host, MyProtocol::SetMaster, &procotol::Null::new()){
                            self.ha_log.save(db)?;
                            return Err(e);
                        };
                        info!("OK");
                    }else {
                        info!("send change master info to slave node: {}....", &slave.host);
                        if let Err(e) = self.exec_change(&slave.host, MyProtocol::ChangeMaster, &change_master_info){
                            self.ha_log.save(db)?;
                            return Err(e);
                        };
                        info!("OK");
                    }
                }
            //}else {
                //client在线、判断是否有需要追加的数据

            //}
        }
        self.get_recovery_info()?;
        Ok(())
    }

    ///
    /// 通过read_binlog信息选举新master
    /// 
    fn elc_new_master(&mut self) -> ChangeMasterInfo{
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
        let cm = ChangeMasterInfo{ master_host: host_vec[0].to_string(), master_port: dbport};
        return cm;
    }

    ///
    /// 从新master获取读取的binlog信息以及gtid信息
    /// 
    fn get_recovery_info(&mut self) -> Result<(), Box<dyn Error>> {
        for node in &self.slave_nodes {
            if node.new_master {
                self.ha_log.recovery_info = RecoveryInfo::new(node.host.clone(), node.dbport.clone())?;
//                let mut conn = crate::ha::conn(&node.host)?;
//                let host_info = node.host.split(":");
//                let host_vec = host_info.collect::<Vec<&str>>();
//                //let mut buf: Vec<u8> = vec![];
//                //buf.push(0xf3);
//                //send_packet(&buf, &mut conn)?;
//                send_value_packet(&mut conn, &procotol::Null::new(), MyProtocol::GetRecoveryInfo)?;
//                let packet = rec_packet(&mut conn)?;
//                let type_code = MyProtocol::new(&packet[0]);
//                match type_code {
//                    MyProtocol::GetRecoveryInfo => {
//                        let value: GetRecoveryInfo = serde_json::from_slice(&packet[9..])?;
//                        self.ha_log.recovery_info = RecoveryInfo::new(value, host_vec[0].to_string(), node.dbport.clone());
//                        return Ok(());
//                    }
//                    _ => {
//                        let a = format!("return invalid type code: {}",&packet[0]);
//                        return  Box::new(Err(a)).unwrap();
//                    }
//                }
            }
        }
        Ok(())
    }

    ///
    /// 对slave执行change或setmaster操作
    ///
    fn exec_change<T: Serialize>(&self, host: &String, type_code: MyProtocol, value: &T) -> Result<(), Box<dyn Error>> {
        let mut conn = crate::ha::conn(host)?;
        //let mut buf: Vec<u8> = vec![];
        //buf.push(type_code.get_code());
        //send_packet(&buf, &mut conn)?;
        send_value_packet(&mut conn, value, type_code)?;
        let packet = rec_packet(&mut conn)?;
        let type_code = MyProtocol::new(&packet[0]);
        match type_code {
            MyProtocol::Ok => {
                return Ok(());
            }
            MyProtocol::Error => {
                let a = format!("{}: {:?}",host, serde_json::from_slice(&packet[9..])?);
                info!("{}",a);
                return Box::new(Err(a)).unwrap();
            }
            _ => {
                let a = format!("return invalid type code: {}",&packet[0]);
                return  Box::new(Err(a)).unwrap();
            }
        }
    }
}




///
/// 分发每个node检查宕机节点状态
///
fn get_down_state_from_node(host_info: &String,
                            down_node: &procotol::DownNodeCheck,
                            sender: Arc<Mutex<mpsc::Sender<DownNodeCheckStatus>>>) {
    let mut conn = conn(host_info).unwrap();
    procotol::send_value_packet(&mut conn, down_node, MyProtocol::DownNodeCheck).unwrap();
    let packet = rec_packet(&mut conn).unwrap();
    let type_code = MyProtocol::new(&packet[0]);
    match type_code {
        MyProtocol::DownNodeCheck => {
            let value: DownNodeCheckStatus = serde_json::from_slice(&packet[9..]).unwrap();
            info!("{}: {:?}",host_info,value);
            sender.lock().unwrap().send(value).unwrap();
        }
        _ => {
            info!("return invalid type code: {}",&packet[0]);
        }
    }

}

///
/// 判断集群内slave节点状态
/// 根据读取binlog位置情况选举新master
/// 并对其余节点执行changemaster
///
#[derive(Serialize, Clone)]
pub struct SlaveInfo {
    pub host: String,
    pub dbport: usize,
    pub slave_info: ReplicationState,
    pub new_master: bool,
}
impl SlaveInfo {
    fn new(host: String, dbport: usize, db: &web::Data<DbInfo>) -> Result<SlaveInfo, Box<dyn Error>> {
        let cf_name = String::from("Nodes_state");
        let node_info = db.get(&host, &cf_name)?;
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
    pub repl_info: RecoveryInfo,
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
            repl_info: RecoveryInfo {
                binlog: "".to_string(),
                position: 0,
                gtid: "".to_string(),
                masterhost: "".to_string(),
                masterport: 0
            }
        }
    }

    pub fn switch(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        let cf_name = String::from("Ha_nodes_info");
        let node_info = db.get(&self.host, &cf_name)?;
        let node_info: HostInfoValue = serde_json::from_str(&node_info.value)?;
        self.cluster_name = node_info.cluster_name;
        self.dbport = node_info.dbport;
        self.get_all_nodes_for_cluster_name(&db, &cf_name)?;
        self.set_master_variables()?;
        self.get_repl_info()?;
        self.run_switch()?;
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
                    return Box::new(Err(String::from("do not allow the current master to perform this operation"))).unwrap();
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
        let mut conn = conn(&self.old_master_info.host)?;
        procotol::send_value_packet(&mut conn, &procotol::Null::new(), MyProtocol::SetVariables)?;
        if let Err(e) = self.rec_info(&mut conn){
            let a = format!("set old master {} variables error: {}",&self.old_master_info.host,e);
            return Box::new(Err(a)).unwrap();
        };
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
        loop {
            let state = get_node_state_from_host(&self.host)?;
            if state.seconds_behind > 0 { continue; };
            self.repl_info = RecoveryInfo::new(self.host.clone(), self.dbport.clone())?;
            break;
        }
        Ok(())
    }

    fn run_switch(&mut self) -> Result<(), Box<dyn Error>> {
        let mut err_host = vec![];
        for slave in &self.slave_nodes_info {
            if let Err(e) = self.run_change_to_node(slave){
                info!("host: {}, change error: {}",&slave.host, e.to_string());
                err_host.push(slave.host.clone());
            };
        }
        if err_host.len() > 0 {
            let err = format!("switch failed host list : {:?}", err_host);
            return Box::new(Err(err)).unwrap();
        }

        //切换旧master为slave
        if let Err(e) = self.run_change_to_node(&self.old_master_info){
            let err = format!("switch failed host list : {:?} {:?}", err_host, e.to_string());
            return Box::new(Err(err)).unwrap();
        }

        let mut conn = conn(&self.host)?;
        send_value_packet(&mut conn, &procotol::Null::new(), MyProtocol::SetMaster)?;
        self.rec_info(&mut conn)?;
        return Ok(());
    }

    fn run_change_to_node(&self, node: &HostInfoValueGetAllState) -> Result<(), Box<dyn Error>> {
        let mut conn = conn(&node.host)?;
        send_value_packet(&mut conn, &self.repl_info, MyProtocol::RecoveryCluster)?;
        self.rec_info(&mut conn)?;
        Ok(())
    }

    ///
    /// 切换失败恢复master状态
    ///
    fn recovery_variables(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn rec_info(&self, conn: &mut TcpStream) -> Result<(), Box<dyn Error>> {
        let packet = rec_packet(conn)?;
        let type_code = MyProtocol::new(&packet[0]);
        match type_code {
            MyProtocol::Error => {
                let err: procotol::ReponseErr = serde_json::from_slice(&packet[9..])?;
                return  Box::new(Err(err.err)).unwrap();
            }
            _ =>{}
        }
        Ok(())
    }

}





