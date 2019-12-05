/*
@author: xiao cai niao
@datetime: 2019/11/6
*/
use actix_web::{web};
use crate::webroute::route::HostInfo;
use crate::storage::rocks::{DbInfo, KeyValue, CfNameTypeCode};
use crate::ha::procotol::{DownNodeCheck, RecoveryInfo, ReplicationState};
use std::error::Error;
use crate::ha::nodes_manager::SlaveInfo;
use serde::{Serialize, Deserialize};


///
/// mysql node info， insert to rocksdb
///
///

pub fn insert_mysql_host_info(data: web::Data<DbInfo>, info: &web::Form<HostInfo>) -> Result<(), Box<dyn Error>> {
    let cf_name = String::from("Ha_nodes_info");
    let key = &info.host;

    let check_unique = data.get(key, &cf_name);
    match check_unique {
        Ok(v) => {
            if v.value.len() > 0 {
                let a = format!("this key: ({}) already exists in the database",key);
                return Box::new(Err(a)).unwrap();
            }
        }
        _ => {}
    }
    info!("{:?}",info);
    //info.create_time = crate::timestamp() as i64;
    let v = crate::ha::procotol::HostInfoValue{
        host: info.host.clone(),
        rtype: info.rtype.clone(),
        dbport: info.dbport.parse::<usize>()?,
        cluster_name: info.cluster_name.clone(),
        online: false,
        insert_time: crate::timestamp(),
        update_time: crate::timestamp(),
        maintain: false
    };

    let value = serde_json::to_string(&v)?;
    let row = KeyValue{key: key.parse()?, value};
    let a = data.put(&row, &cf_name);
    match a {
        Ok(()) => Ok(()),
        Err(e) => {
            return Box::new(Err(e.to_string())).unwrap();
        }
    }

}

#[derive(Serialize, Deserialize)]
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
                masterport: 0
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